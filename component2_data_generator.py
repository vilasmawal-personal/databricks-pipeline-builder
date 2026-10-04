"""
DPBA — Component 2: Sample Data Generator Agent
=================================================
Reads a PipelineSpec (parsed from the canonical universal mapping JSON) and
generates:

  1. synthetic_input.csv    — raw source rows (what would arrive from upstream)
  2. expected_output.csv    — what Silver should look like after transformation
                               (only rows with expected_disposition == "silver")
  3. test_cases.json        — metadata: test ID, scenario, edge case, and
                               expected_disposition ("silver" | "quarantine" | "dropped")
  4. load_test_fixtures.py  — a generated Databricks notebook that writes (1)
                               and (2) as real Unity Catalog Delta tables
                               (spec.test_synthetic_input_table /
                               spec.test_expected_output_table), so the
                               integration test notebook queries actual UC
                               fixtures instead of parsing a JSON file shipped
                               separately on DBFS — no manual upload step.

This generator is fully spec-driven: every edge case is derived from a
field's *explicit* `transformation_type` and `transformation_params` (the
same fields Component 3 compiles to PySpark — see mapping_parser.classify
vocabulary), never from free-text sniffing. Expected values are *computed*
by mirroring each transformation type's exact Spark semantics (see
_compute_raw_expected below), not looked up from a parallel hardcoded table —
this is what guarantees Component 2's test oracle can never silently drift
from what Component 3's generated code actually does.

Edge cases covered, per field behavior:
  - Happy path, seeded from test_data_specifications.sample_inputs/
    expected_outputs when the mapping JSON provides them, else synthesized.
  - cast        → padding/trim, very long value, numeric-looking value
  - concat      → each source null in turn, all-null, special characters
  - conditional → zero/negative/large/non-numeric/empty/null boundary values
  - date_format / conditional_date → each declared source format, unparseable,
    empty, invalid calendar date, (conditional_date) the sentinel value
  - lookup      → every declared enum key (case variants), unexpected value
  - split       → delimiter present / absent
  - data_expectations[] → one deliberately-violating row per drop_row/
    quarantine expectation, tagged with its exact expected_disposition
    (fail_pipeline expectations are intentionally NOT exercised here — see
    the note in component3_pipeline_builder.generate_integration_test_notebook)
  - Primary key → every independent test row gets a unique PK value (except
    the deliberate duplicate-PK pair), since Silver's dedup/MERGE is keyed on it
  - Duplicates  → two rows sharing one PK → only the latest should survive
  - Whitespace  → padding across every string-ish field at once

Human-in-the-Loop:
  After generating, the agent displays a preview table and asks the user to
  approve, edit (by modifying the CSV directly), or reject before proceeding.
"""

from __future__ import annotations

import csv
import json
import os
import re
import textwrap
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from dpba.models.pipeline_spec import (
    FieldMapping, PipelineSpec, PipelineStage, DataExpectation
)
from mapping_parser import (
    parse_mapping_json, print_spec_table,
    T_DIRECT, T_CAST, T_CONCAT, T_CONDITIONAL, T_DATE_FORMAT, T_CONDITIONAL_DATE,
    T_LOOKUP, T_SPLIT,
)


# ─────────────────────────────────────────────────────────────────────────────
# Test Case Model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TestCase:
    tc_id:        str
    scenario:     str
    edge_case:    str
    input_row:    dict[str, Any]
    expected_row: dict[str, Any]
    expected_disposition: str = "silver"   # "silver" | "quarantine" | "dropped"


# ─────────────────────────────────────────────────────────────────────────────
# Spark date-pattern ↔ Python strptime/strftime translation
# ─────────────────────────────────────────────────────────────────────────────

_DATE_TOKEN_MAP = [("yyyy", "%Y"), ("MM", "%m"), ("dd", "%d"), ("HH", "%H"), ("mm", "%M"), ("ss", "%S")]


def _spark_pattern_to_py(pattern: str) -> str:
    out, i = [], 0
    while i < len(pattern):
        if pattern[i] == "'":
            j = pattern.find("'", i + 1)
            if j == -1:
                out.append(pattern[i + 1:])
                break
            out.append(pattern[i + 1:j])
            i = j + 1
            continue
        matched = False
        for tok, rep in _DATE_TOKEN_MAP:
            if pattern.startswith(tok, i):
                out.append(rep)
                i += len(tok)
                matched = True
                break
        if not matched:
            out.append(pattern[i])
            i += 1
    return "".join(out)


def _parse_and_format_date(raw: Optional[str], params: dict) -> Optional[str]:
    if not raw:
        return None
    source_formats = params.get("source_formats") or ["yyyyMMdd", "yyyy/MM/dd", "yyyy-MM-dd", "MM/dd/yyyy"]
    output_format = params.get("output_format")
    parsed = None
    for fmt in source_formats:
        try:
            parsed = datetime.strptime(str(raw), _spark_pattern_to_py(fmt))
            break
        except ValueError:
            continue
    if parsed is None:
        return None
    if output_format:
        return parsed.strftime(_spark_pattern_to_py(output_format))
    return parsed.date().isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Expected-value calculator — mirrors each transformation_type's exact Spark
# semantics from component3_pipeline_builder.SnippetLibrary, in Python.
# ─────────────────────────────────────────────────────────────────────────────

def _compute_raw_expected(fm: FieldMapping, raws: dict[str, Any]) -> Any:
    t = fm.transformation_type

    if t == T_DIRECT:
        if not fm.source_fields:
            return None
        v = raws.get(fm.primary_source)
        return None if v is None else str(v)

    if t == T_CAST:
        v = raws.get(fm.primary_source)
        if v is None:
            return None
        s = str(v).strip()
        if fm.transformation_params.get("uppercase"):
            s = s.upper()
        elif fm.transformation_params.get("lowercase"):
            s = s.lower()
        return s

    if t == T_CONCAT:
        sep = fm.transformation_params.get("separator", " ")
        vals = [raws.get(s) for s in fm.source_fields]
        if all(v is None for v in vals):
            return None
        return sep.join(str(v).strip() for v in vals if v is not None)

    if t == T_CONDITIONAL:
        divisor = fm.transformation_params.get("divisor", 1)
        v = raws.get(fm.primary_source)
        try:
            return round(int(str(v)) / divisor, 2)
        except (TypeError, ValueError):
            return None

    if t == T_DATE_FORMAT:
        return _parse_and_format_date(raws.get(fm.primary_source), fm.transformation_params)

    if t == T_CONDITIONAL_DATE:
        v = raws.get(fm.primary_source)
        sentinel = fm.transformation_params.get("sentinel_value")
        if sentinel is not None and v is not None and str(v).strip() == sentinel:
            return None
        return _parse_and_format_date(v, fm.transformation_params)

    if t == T_LOOKUP:
        v = raws.get(fm.primary_source)
        if v is None:
            return None
        value_map = fm.transformation_params.get("value_map", {})
        case_insensitive = fm.transformation_params.get("case_insensitive", True)
        key_cmp = str(v).strip().upper() if case_insensitive else str(v).strip()
        for k, mapped in value_map.items():
            k_cmp = str(k).upper() if case_insensitive else str(k)
            if key_cmp == k_cmp:
                return mapped
        return None

    if t == T_SPLIT:
        v = raws.get(fm.primary_source)
        if v is None:
            return None
        delimiter = fm.transformation_params.get("delimiter", " ")
        index = int(fm.transformation_params.get("index", 0))
        parts = str(v).split(delimiter)
        return parts[index].strip() if 0 <= index < len(parts) else None

    raise ValueError(f"No expected-value calculator for transformation_type={t!r}")


def _expected_value(fm: FieldMapping, raws: dict[str, Any]) -> Any:
    """Raw computed value, then the SAME universal default-value coalesce
    Component 3 applies after every snippet."""
    raw_result = _compute_raw_expected(fm, raws)
    if raw_result is None and fm.default_value is not None:
        return fm.default_value
    return raw_result


# ─────────────────────────────────────────────────────────────────────────────
# Happy-path raw-value synthesis
# ─────────────────────────────────────────────────────────────────────────────

_FIRST_POOL = ["Alice", "Jordan", "Priya", "Marcus", "Renée"]
_LAST_POOL  = ["Smith", "Chen", "Nair", "Johnson", "O'Brien"]


def _happy_raw(fm: FieldMapping, index: int) -> dict[str, Any]:
    t = fm.transformation_type

    if t == T_CONCAT:
        pools = [_FIRST_POOL, _LAST_POOL]
        raws = {}
        for i, src in enumerate(fm.source_fields):
            pool = pools[i] if i < len(pools) else _FIRST_POOL
            raws[src] = pool[index % len(pool)]
        return raws

    if t == T_LOOKUP:
        value_map = fm.transformation_params.get("value_map", {})
        key = next(iter(value_map), "Y")
        return {fm.primary_source: key}

    if t == T_CONDITIONAL:
        return {fm.primary_source: "125050"}

    if t in (T_DATE_FORMAT, T_CONDITIONAL_DATE):
        fmt = (fm.transformation_params.get("source_formats") or ["yyyy-MM-dd"])[0]
        return {fm.primary_source: datetime(2023, 1, 15).strftime(_spark_pattern_to_py(fmt))}

    if t == T_SPLIT:
        delimiter = fm.transformation_params.get("delimiter", " ")
        return {fm.primary_source: f"PART-A{delimiter}PART-B"}

    if t == T_DIRECT:
        if not fm.source_fields:
            return {}
        return {fm.primary_source: f"{fm.target_field.upper()[:8]}-{index:04d}"}

    # T_CAST and anything else
    return {fm.primary_source: f"{fm.target_field.upper()[:8]}-{index:04d}"}


# ─────────────────────────────────────────────────────────────────────────────
# Expectation-violation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _field_by_target(stage: PipelineStage, target_field: str) -> Optional[FieldMapping]:
    for fm in stage.fields:
        if fm.target_field == target_field:
            return fm
    return None


def _violating_target_value(op: str, value: float) -> float:
    return {
        ">=": value - 1, ">": value,
        "<=": value + 1, "<": value,
        "==": value + 1, "=": value + 1,
        "!=": value,
    }[op]


def _rule_satisfied(actual: Any, op: str, value: Optional[float]) -> bool:
    if op == "IS NOT NULL":
        return actual is not None
    if op == "IS NULL":
        return actual is None
    if actual is None:
        return True   # a null can't violate a numeric comparison here — IS NOT NULL covers that case separately
    try:
        actual_num = float(actual)
    except (TypeError, ValueError):
        return True
    return {
        ">=": actual_num >= value, "<=": actual_num <= value,
        ">": actual_num > value, "<": actual_num < value,
        "==": actual_num == value, "=": actual_num == value,
        "!=": actual_num != value,
    }[op]


def _raw_value_for_numeric_target(fm: FieldMapping, desired_target_value: float) -> str:
    """Best-effort inverse of _compute_raw_expected for numeric fields — produces
    a raw input that computes to ~desired_target_value, to synthesize a row that
    deliberately violates a numeric data_expectation."""
    if fm.transformation_type == T_CONDITIONAL:
        divisor = fm.transformation_params.get("divisor", 100)
        return str(int(round(desired_target_value * divisor)))
    return str(desired_target_value)


# ─────────────────────────────────────────────────────────────────────────────
# Test case generator
# ─────────────────────────────────────────────────────────────────────────────

class DataGenerator:
    """
    Per the hackathon's own flexibility requirement, a pipeline is not
    hardcoded to a single bronze→silver hop — it's an arbitrary JSON-declared
    CHAIN of transform stages (spec.transform_stages). Every generated test
    row's expected outcome is computed by walking that *entire* chain
    end-to-end (see _compute_chain_and_disposition), so multi-stage pipelines
    (e.g. bronze→silver→gold) are validated correctly, not just the first hop.

    Exhaustive edge-case FUZZING (null/boundary/type-mismatch/format variants)
    is generated only against the FIRST transform stage's fields — that's
    where real-world messy data actually enters the pipeline. Crafting a raw
    input that forces a specific value at a *later* stage would require
    inverting arbitrary upstream transformations (not always well-defined,
    e.g. through a lookup or concat), so later stages are instead validated
    for correctness via the happy-path/seeded rows chaining through them
    end-to-end, plus row-count sanity checks in the generated integration
    test notebook — an honest guarantee rather than faked coverage.
    """

    def __init__(self, spec: PipelineSpec):
        self.spec = spec
        self._tc_counter = 0
        self._pk_seq = 0
        self.primary_stage = spec.transform_stages[0]
        self._pk_fm = spec.primary_key_field
        # The primary key is assumed to be established at the first transform
        # stage (overwhelmingly the common case) and carried through
        # unchanged by later stages.
        self._pk_source_fm = _field_by_target(self.primary_stage, self._pk_fm.target_field)

        self._field_info: dict[str, dict[str, Any]] = {}
        for i, fm in enumerate(self.primary_stage.fields):
            self._field_info[fm.target_field] = {"fm": fm, "happy_raw": _happy_raw(fm, i + 1)}

        self.happy_input_row: dict[str, Any] = {}
        for meta in self._field_info.values():
            self.happy_input_row.update(meta["happy_raw"])

    def _next_id(self) -> str:
        self._tc_counter += 1
        return f"TC-{self._tc_counter:02d}"

    def _resolve_stage_disposition(self, stage: PipelineStage, computed_row: dict[str, Any]) -> tuple[str, bool]:
        """Check one stage's own data_expectations against its computed output.
        Returns (disposition, fail_pipeline_conflict)."""
        for exp in stage.expectations:
            if not exp.parsed:
                continue
            actual = computed_row.get(exp.parsed["field"])
            if _rule_satisfied(actual, exp.parsed["op"], exp.parsed["value"]):
                continue
            if exp.action_on_failure == "quarantine":
                return "quarantine", False
            if exp.action_on_failure == "drop_row":
                return "dropped", False
            if exp.action_on_failure == "fail_pipeline":
                # A single such row would raise in that stage's transform
                # notebook and abort the ENTIRE test batch — never ship one.
                return "silver", True
        return "silver", False

    def _compute_chain_and_disposition(self, source_raws: dict[str, Any]) -> tuple[dict[str, Any], str, bool]:
        """Walk every transform stage in order, feeding each stage's computed
        output forward as the next stage's input — exactly how the real
        generated pipeline executes. If a stage's expectations reject the
        row, it never reaches later stages (disposition is returned early)."""
        current = source_raws
        for stage in self.spec.transform_stages:
            stage_out = {fm.target_field: _expected_value(fm, current) for fm in stage.fields}
            disposition, conflict = self._resolve_stage_disposition(stage, stage_out)
            if conflict:
                return {}, "silver", True
            if disposition != "silver":
                return {}, disposition, False
            current = stage_out
        return current, "silver", False

    def _unique_pk_override(self) -> dict[str, Any]:
        """A fresh, never-reused raw value for whatever stage-1 field
        ultimately becomes the final stage's primary key, so each independent
        test row is distinctly addressable under dedup/MERGE. If the pk isn't
        established at stage 1 (unusual), this is a no-op — see class docstring."""
        if self._pk_source_fm is None:
            return {}
        self._pk_seq += 1
        return {self._pk_source_fm.primary_source: f"PK-{self._pk_seq:04d}"}

    def _make_case(
        self, scenario: str, edge_case: str,
        raw_overrides: Optional[dict[str, Any]] = None,
        fresh_pk: bool = True,
    ) -> TestCase:
        input_row = dict(self.happy_input_row)
        if fresh_pk:
            input_row.update(self._unique_pk_override())
        if raw_overrides:
            input_row.update(raw_overrides)

        final_row, disposition, conflict = self._compute_chain_and_disposition(input_row)
        if conflict:
            disposition = "_exclude_"
        expected_row = final_row if disposition == "silver" else {}

        return TestCase(
            tc_id=self._next_id(), scenario=scenario, edge_case=edge_case,
            input_row=input_row, expected_row=expected_row, expected_disposition=disposition,
        )

    # ── Generate all test cases ───────────────────────────────────────────────

    def generate(self) -> list[TestCase]:
        tcs: list[TestCase] = []
        tcs += self._seeded_cases()
        tcs += [self._make_case("Happy path — all fields present, valid, and well-formed", "happy_path")]
        tcs += self._expectation_cases()
        for fm in self.primary_stage.fields:
            tcs += self._field_variant_cases(fm)
        tcs += self._duplicate_cases()
        tcs += self._whitespace_case()
        # Safety net: never ship a row that would trip a fail_pipeline expectation —
        # see _resolve_stage_disposition's docstring.
        return [tc for tc in tcs if tc.expected_disposition != "_exclude_"]

    # ── Seed from test_data_specifications (mapping JSON's own fixtures) ──────
    def _seeded_cases(self) -> list[TestCase]:
        tcs = []
        for i, sample_input in enumerate(self.spec.sample_inputs):
            input_row = dict(self.happy_input_row)
            input_row.update(sample_input)
            if i < len(self.spec.sample_expected_outputs):
                expected_row = dict(self.spec.sample_expected_outputs[i])
                disposition = "silver"
            else:
                expected_row, disposition, conflict = self._compute_chain_and_disposition(input_row)
                if conflict:
                    continue
                if disposition != "silver":
                    expected_row = {}
            tcs.append(TestCase(
                tc_id=self._next_id(),
                scenario=f"Seed sample #{i + 1} from test_data_specifications (mapping JSON)",
                edge_case="seed_sample",
                input_row=input_row, expected_row=expected_row, expected_disposition=disposition,
            ))
        return tcs

    # ── Deliberately violate each of stage 1's drop_row / quarantine expectations ──
    def _expectation_cases(self) -> list[TestCase]:
        tcs = []
        for exp in self.primary_stage.expectations:
            if exp.action_on_failure == "fail_pipeline" or not exp.parsed:
                continue  # fail_pipeline: see class docstring; unparsed: nothing to encode

            target_fm = _field_by_target(self.primary_stage, exp.parsed["field"])
            if target_fm is None:
                continue
            is_pk_field = target_fm.target_field == self._pk_fm.target_field
            op, value = exp.parsed["op"], exp.parsed["value"]

            if op == "IS NOT NULL":
                raw_overrides = {s: None for s in target_fm.source_fields} if target_fm.source_fields else {}
            elif op == "IS NULL":
                raw_overrides = {}   # happy-path value is already non-null → already violates "IS NULL"
            else:
                violating_val = _violating_target_value(op, value)
                raw_overrides = {target_fm.primary_source: _raw_value_for_numeric_target(target_fm, violating_val)}

            tcs.append(self._make_case(
                f'Expectation "{exp.name}" violated: {exp.rule}',
                f"expectation_{exp.action_on_failure}",
                raw_overrides=raw_overrides, fresh_pk=not is_pk_field,
            ))
        return tcs

    # ── Per-field transformation-type variants (stage 1 only — see class docstring) ─
    def _field_variant_cases(self, fm: FieldMapping) -> list[TestCase]:
        t = fm.transformation_type
        is_pk_field = fm.target_field == self._pk_fm.target_field
        tcs: list[TestCase] = []

        if t == T_CONDITIONAL:
            for raw, label, edge in [
                ("0", "zero boundary", "boundary"),
                (None, "null → default", "null_optional"),
                ("-500", "negative value", "boundary"),
                ("999999900", "large value", "boundary"),
                ("abc", "non-numeric → default", "type_mismatch"),
                ("", "empty string → default", "type_mismatch"),
            ]:
                tcs.append(self._make_case(
                    f"{fm.target_field}: {label}", edge,
                    raw_overrides={fm.primary_source: raw}, fresh_pk=not is_pk_field,
                ))

        elif t == T_LOOKUP:
            value_map = fm.transformation_params.get("value_map", {})
            for key in value_map:
                for variant, label, edge in [
                    (str(key).lower(), f"lowercase '{key}'", "case_sensitivity"),
                    (str(key).upper(), f"uppercase '{key}'", "case_sensitivity"),
                ]:
                    tcs.append(self._make_case(
                        f"{fm.target_field}: {label}", edge,
                        raw_overrides={fm.primary_source: variant}, fresh_pk=not is_pk_field,
                    ))
            for raw, label, edge in [("", "empty → null", "null_optional"), ("ZZ", "unexpected value → null", "type_mismatch")]:
                tcs.append(self._make_case(
                    f"{fm.target_field}: {label}", edge,
                    raw_overrides={fm.primary_source: raw}, fresh_pk=not is_pk_field,
                ))

        elif t in (T_DATE_FORMAT, T_CONDITIONAL_DATE):
            source_formats = fm.transformation_params.get("source_formats") or ["yyyyMMdd", "yyyy/MM/dd", "yyyy-MM-dd", "MM/dd/yyyy"]
            for fmt in source_formats:
                raw = datetime(2023, 1, 15).strftime(_spark_pattern_to_py(fmt))
                tcs.append(self._make_case(
                    f"{fm.target_field}: source format '{fmt}'", "date_format",
                    raw_overrides={fm.primary_source: raw}, fresh_pk=not is_pk_field,
                ))
            for raw, label, edge in [
                ("not-a-date", "unparseable value", "type_mismatch"),
                ("", "empty value", "null_optional"),
                ("20231301", "invalid month (13)", "type_mismatch"),
            ]:
                tcs.append(self._make_case(
                    f"{fm.target_field}: {label}", edge,
                    raw_overrides={fm.primary_source: raw}, fresh_pk=not is_pk_field,
                ))
            sentinel = fm.transformation_params.get("sentinel_value")
            if sentinel:
                tcs.append(self._make_case(
                    f"{fm.target_field}: sentinel value '{sentinel}' → null", "boundary",
                    raw_overrides={fm.primary_source: sentinel}, fresh_pk=not is_pk_field,
                ))

        elif t == T_CAST:
            padded = f"  {fm.target_field.upper()[:10]}-PAD  "
            for raw, label, edge in [
                (padded, "padded value → trimmed", "whitespace"),
                ("A" * 255, "very long value (255 chars)", "boundary"),
                ("12345", "numeric-looking value cast to string", "type_cast"),
            ]:
                tcs.append(self._make_case(
                    f"{fm.target_field}: {label}", edge,
                    raw_overrides={fm.primary_source: raw}, fresh_pk=not is_pk_field,
                ))

        elif t == T_CONCAT:
            tcs += self._concat_variant_cases(fm)

        elif t == T_SPLIT:
            delimiter = fm.transformation_params.get("delimiter", " ")
            tcs.append(self._make_case(
                f"{fm.target_field}: delimiter '{delimiter}' absent from source", "type_mismatch",
                raw_overrides={fm.primary_source: "SINGLEVALUE"}, fresh_pk=not is_pk_field,
            ))

        return tcs

    def _concat_variant_cases(self, fm: FieldMapping) -> list[TestCase]:
        tcs = []
        for src in fm.source_fields:
            tcs.append(self._make_case(
                f"{fm.target_field}: {src} is null → remaining field(s) only", "null_optional",
                raw_overrides={src: None},
            ))

        tcs.append(self._make_case(
            f"{fm.target_field}: all source fields null → {'default' if fm.default_value is not None else 'null'}",
            "null_optional",
            raw_overrides={s: None for s in fm.source_fields},
        ))

        special_pool = ["José", "O'Brien", "Renée", "Mary-Jane", "Nguyễn"]
        special_raws = {src: special_pool[i % len(special_pool)] for i, src in enumerate(fm.source_fields)}
        tcs.append(self._make_case(
            f"{fm.target_field}: special characters (accents, apostrophes, hyphens)", "special_chars",
            raw_overrides=special_raws,
        ))

        return tcs

    # ── Duplicates ──────────────────────────────────────────────────────────────
    def _duplicate_cases(self) -> list[TestCase]:
        if self._pk_source_fm is None:
            return []   # can't safely construct a guaranteed-duplicate pair — see class docstring
        overrides = {self._pk_source_fm.primary_source: "DUP-0001"}
        return [
            self._make_case(
                "Duplicate primary key (first insert) — should be deduplicated away",
                "duplicate", raw_overrides=overrides, fresh_pk=False,
            ),
            self._make_case(
                "Duplicate primary key (second / latest insert) — survives dedup",
                "duplicate", raw_overrides=overrides, fresh_pk=False,
            ),
        ]

    # ── Whitespace / padding across every string-ish field at once ──────────────
    def _whitespace_case(self) -> list[TestCase]:
        raw_overrides: dict[str, Any] = {}
        pk_touched = False

        for meta in self._field_info.values():
            fm = meta["fm"]
            if fm.transformation_type == T_CONCAT:
                for src in fm.source_fields:
                    raw_overrides[src] = f"  {meta['happy_raw'][src]}  "
                if fm.target_field == self._pk_fm.target_field:
                    pk_touched = True
            elif fm.transformation_type == T_CAST:
                raw_overrides[fm.primary_source] = f"  {meta['happy_raw'][fm.primary_source]}  "
                if fm.target_field == self._pk_fm.target_field:
                    pk_touched = True

        return [self._make_case(
            "Whitespace/padding on all string-ish fields — trim should be applied",
            "whitespace", raw_overrides=raw_overrides, fresh_pk=not pk_touched,
        )]


# ─────────────────────────────────────────────────────────────────────────────
# Writers
# ─────────────────────────────────────────────────────────────────────────────

def _all_input_columns(tcs: list[TestCase]) -> list[str]:
    cols = []
    for tc in tcs:
        for k in tc.input_row:
            if k not in cols:
                cols.append(k)
    return cols

def _all_output_columns(tcs: list[TestCase]) -> list[str]:
    cols = []
    for tc in tcs:
        for k in tc.expected_row:
            if k not in cols:
                cols.append(k)
    return cols

def write_synthetic_input_csv(tcs: list[TestCase], path: str) -> None:
    cols = _all_input_columns(tcs)
    meta = ["_tc_id", "_scenario", "_edge_case", "_expected_disposition"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=meta + cols, extrasaction="ignore")
        w.writeheader()
        for tc in tcs:
            row = {
                "_tc_id": tc.tc_id, "_scenario": tc.scenario, "_edge_case": tc.edge_case,
                "_expected_disposition": tc.expected_disposition,
            }
            row.update({k: ("" if v is None else str(v)) for k, v in tc.input_row.items()})
            w.writerow(row)
    print(f"  [data_gen] ✅ Wrote {len(tcs)} rows → {path}")

def write_expected_output_csv(tcs: list[TestCase], path: str) -> None:
    silver_tcs = [tc for tc in tcs if tc.expected_disposition == "silver"]
    cols = _all_output_columns(tcs)
    meta = ["_tc_id", "_scenario", "_edge_case"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=meta + cols, extrasaction="ignore")
        w.writeheader()
        for tc in silver_tcs:
            row = {"_tc_id": tc.tc_id, "_scenario": tc.scenario, "_edge_case": tc.edge_case}
            row.update({k: ("" if v is None else str(v)) for k, v in tc.expected_row.items()})
            w.writerow(row)
    print(f"  [data_gen] ✅ Wrote {len(silver_tcs)} expected Silver rows → {path}")

def write_test_cases_json(tcs: list[TestCase], path: str) -> None:
    payload = [
        {
            "tc_id": tc.tc_id, "scenario": tc.scenario, "edge_case": tc.edge_case,
            "expected_disposition": tc.expected_disposition,
            "input_row": {k: (None if v is None else str(v)) for k, v in tc.input_row.items()},
            "expected_row": {k: (None if v is None else str(v)) for k, v in tc.expected_row.items()},
        }
        for tc in tcs
    ]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"  [data_gen] ✅ Wrote test case metadata → {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Unity Catalog fixture-loading notebook
# ─────────────────────────────────────────────────────────────────────────────
# Component 2 owns the DATA (it already computed every row); this generates
# the Databricks notebook that puts that data into Unity Catalog as real
# Delta tables, so the integration test notebook — and anyone exploring the
# catalog — can query actual fixtures instead of a JSON file that has to be
# manually copied to DBFS before every run.

def _reindent(block: str, indent: str = "        ") -> str:
    """See the identical helper in component3_pipeline_builder.py: f-string
    substitution only places a value's FIRST line at the template's column;
    continuation lines need this explicit prefix or textwrap.dedent's
    common-whitespace calculation (and the resulting indentation) breaks."""
    return f"\n{indent}".join(block.split("\n"))


def generate_load_fixtures_notebook(spec: PipelineSpec, tcs: list[TestCase]) -> str:
    """Writes Component 2's full generated test suite — every synthetic input
    row and every expected final-stage output row — as two Unity Catalog
    Delta tables (spec.test_synthetic_input_table / test_expected_output_table).
    The data is embedded as literal row tuples (Component 2 already computed
    it deterministically), matched explicitly against an all-string schema so
    the write doesn't depend on PySpark's dict-based schema inference."""
    input_cols  = ["_tc_id", "_scenario", "_edge_case", "_expected_disposition"] + _all_input_columns(tcs)
    output_cols = ["_tc_id", "_scenario", "_edge_case"] + _all_output_columns(tcs)
    silver_tcs  = [tc for tc in tcs if tc.expected_disposition == "silver"]

    def _input_row(tc: TestCase) -> dict[str, Any]:
        row = {"_tc_id": tc.tc_id, "_scenario": tc.scenario, "_edge_case": tc.edge_case,
               "_expected_disposition": tc.expected_disposition}
        row.update({k: (None if v is None else str(v)) for k, v in tc.input_row.items()})
        return row

    def _output_row(tc: TestCase) -> dict[str, Any]:
        row = {"_tc_id": tc.tc_id, "_scenario": tc.scenario, "_edge_case": tc.edge_case}
        row.update({k: (None if v is None else str(v)) for k, v in tc.expected_row.items()})
        return row

    input_tuples  = [tuple(_input_row(tc).get(c) for c in input_cols) for tc in tcs]
    output_tuples = [tuple(_output_row(tc).get(c) for c in output_cols) for tc in silver_tcs]

    input_schema_fields  = ",\n".join(f'    StructField("{c}", StringType(), True)' for c in input_cols)
    output_schema_fields = ",\n".join(f'    StructField("{c}", StringType(), True)' for c in output_cols)

    return textwrap.dedent(f"""\
        # Databricks notebook source
        # Auto-generated by DPBA — Component 2: Sample Data Generator Agent
        # ⚠️  DO NOT EDIT MANUALLY — re-generate from the mapping JSON instead

        # COMMAND ----------
        # MAGIC %md
        # MAGIC # 00 — Load Test Fixtures into Unity Catalog
        # MAGIC Writes Component 2's generated synthetic input ({len(tcs)} rows, every
        # MAGIC edge case) and expected output ({len(silver_tcs)} rows) as real Delta
        # MAGIC tables, so the integration test notebook queries actual Unity Catalog
        # MAGIC fixtures — no DBFS file upload step required.
        # MAGIC
        # MAGIC > Generated by DPBA. Source: mapping JSON → test cases → Component 2.

        # COMMAND ----------
        from pyspark.sql.types import StructType, StructField, StringType

        # COMMAND ----------
        # ── Parameters ──────────────────────────────────────────────────────────
        dbutils.widgets.text("synthetic_input_table", "{spec.test_synthetic_input_table}", "Synthetic Input Table")
        dbutils.widgets.text("expected_output_table",  "{spec.test_expected_output_table}",  "Expected Output Table")

        SYNTHETIC_INPUT_TABLE = dbutils.widgets.get("synthetic_input_table")
        EXPECTED_OUTPUT_TABLE = dbutils.widgets.get("expected_output_table")

        # COMMAND ----------
        # ── Ensure the target schema exists (the catalog itself must already exist —
        # ── creating a catalog needs metastore-admin privileges this notebook doesn't assume) ──
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{'.'.join(SYNTHETIC_INPUT_TABLE.split('.')[:2])}}")

        # COMMAND ----------
        # ── Synthetic input: every generated test case's raw source row ──────────
        INPUT_SCHEMA = StructType([
        {_reindent(input_schema_fields)}
        ])
        SYNTHETIC_INPUT_DATA = {input_tuples!r}

        df_synthetic_input = spark.createDataFrame(SYNTHETIC_INPUT_DATA, schema=INPUT_SCHEMA)
        (
            df_synthetic_input
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(SYNTHETIC_INPUT_TABLE)
        )
        print(f"[fixtures] ✅ Wrote {{df_synthetic_input.count()}} rows to {{SYNTHETIC_INPUT_TABLE}}")

        # COMMAND ----------
        # ── Expected output: known-correct final-stage row per non-dropped/-quarantined case ──
        OUTPUT_SCHEMA = StructType([
        {_reindent(output_schema_fields)}
        ])
        EXPECTED_OUTPUT_DATA = {output_tuples!r}

        df_expected_output = spark.createDataFrame(EXPECTED_OUTPUT_DATA, schema=OUTPUT_SCHEMA)
        (
            df_expected_output
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(EXPECTED_OUTPUT_TABLE)
        )
        print(f"[fixtures] ✅ Wrote {{df_expected_output.count()}} rows to {{EXPECTED_OUTPUT_TABLE}}")

        # COMMAND ----------
        import json
        dbutils.notebook.exit(json.dumps({{
            "status": "SUCCESS",
            "synthetic_input_rows": df_synthetic_input.count(),
            "expected_output_rows": df_expected_output.count(),
        }}))
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Human-in-the-Loop Review
# ─────────────────────────────────────────────────────────────────────────────

def human_review(tcs: list[TestCase]) -> bool:
    edge_case_counts: dict[str, int] = {}
    disposition_counts: dict[str, int] = {}
    for tc in tcs:
        edge_case_counts[tc.edge_case] = edge_case_counts.get(tc.edge_case, 0) + 1
        disposition_counts[tc.expected_disposition] = disposition_counts.get(tc.expected_disposition, 0) + 1

    print("\n" + "═" * 70)
    print("  COMPONENT 2 — SAMPLE DATA GENERATOR: REVIEW SUMMARY")
    print("═" * 70)
    print(f"  Total test cases  : {len(tcs)}")
    for disposition, count in sorted(disposition_counts.items()):
        print(f"  → expected {disposition:<11}: {count}")
    print()
    print("  Edge-case coverage:")
    for ec, count in sorted(edge_case_counts.items()):
        print(f"    {ec:<30} {count} test(s)")
    print()
    print("  Test cases preview (first 10):")
    print(f"  {'TC ID':<8} {'Disposition':<12} {'Edge Case':<22} Scenario")
    print("  " + "─" * 90)
    for tc in tcs[:10]:
        scenario_short = tc.scenario[:46] + ("…" if len(tc.scenario) > 46 else "")
        print(f"  {tc.tc_id:<8} {tc.expected_disposition:<12} {tc.edge_case:<22} {scenario_short}")
    if len(tcs) > 10:
        print(f"  ... and {len(tcs) - 10} more")
    print("═" * 70)

    answer = input(
        "\n  [HITL] Approve generated test data? [y=proceed / n=abort / e=show all]: "
    ).strip().lower()

    if answer == "e":
        print()
        for tc in tcs:
            print(f"  {tc.tc_id}  [{tc.expected_disposition}/{tc.edge_case}]  {tc.scenario}")
        answer = input("\n  [HITL] Proceed with this data? [y/N]: ").strip().lower()

    return answer == "y"


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run(spec: PipelineSpec, output_dir: str, skip_hitl: bool = False) -> list[TestCase]:
    print("\n" + "═" * 70)
    print("  COMPONENT 2 — SAMPLE DATA GENERATOR AGENT")
    print("═" * 70)

    gen = DataGenerator(spec)
    tcs = gen.generate()
    print(f"  [data_gen] Generated {len(tcs)} test cases")

    if not skip_hitl:
        approved = human_review(tcs)
        if not approved:
            print("  [data_gen] ❌ Aborted by user — no files written.")
            return []

    os.makedirs(output_dir, exist_ok=True)
    write_synthetic_input_csv(tcs, os.path.join(output_dir, "synthetic_input.csv"))
    write_expected_output_csv(tcs, os.path.join(output_dir, "expected_output.csv"))
    write_test_cases_json(tcs, os.path.join(output_dir, "test_cases.json"))

    print(f"\n  [data_gen] ✅ All outputs written to: {output_dir}")
    return tcs


if __name__ == "__main__":
    import sys
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_json = os.path.join(script_dir, "sample_specs", "customer_pipeline.json")
    json_path = sys.argv[1] if len(sys.argv) > 1 else default_json

    spec = parse_mapping_json(json_path)
    run(spec, output_dir=os.path.join(script_dir, "output", "test_data"))
