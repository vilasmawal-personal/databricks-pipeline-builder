"""
DPBA — Shared: Mapping Parser
==============================
Parses the canonical "universal JSON" structured mapping document (the
declarative hand-off contract between Component 1 — the AI Consultant Agent —
and Components 2/3) into a PipelineSpec dataclass.

The pipeline is NOT hardcoded to a fixed bronze→silver medallion shape — it is
an arbitrary, JSON-declared CHAIN of stages: `stages[0]` is always a raw
ingest (straight copy from the source, no transformation), and `stages[1:]`
are transform stages, each reading the PREVIOUS stage's table and writing its
own, each with its own field mappings and data_expectations. A pipeline can
have 2 stages (ingest→silver), 3 (bronze→silver→gold), or N, with any names.

Preferred, fully flexible schema:

    {
      "pipeline_metadata":    {pipeline_name, description, version, domain},
      "source_configuration": {source_system, format, location, read_options},
      "pipeline_stages": [
        {"stage_name": "bronze", "stage_type": "ingest",
         "target": {table_name, format, write_mode}},
        {"stage_name": "silver", "stage_type": "transform",
         "target": {table_name, format, write_mode, merge_keys},
         "mappings": [...], "data_expectations": [...]},
        {"stage_name": "gold", "stage_type": "transform", ...}   # optional, any number
      ],
      "test_data_specifications": {sample_inputs, expected_outputs},
      "bundle_metadata": {...}, "workflow_specification": {...}   # optional
    }

Legacy shorthand (still accepted, auto-upgraded to a 2-stage pipeline): a top
-level `target_configurations: {bronze_layer, silver_layer}` plus top-level
`mappings[]`/`data_expectations[]`, exactly as the hackathon's reference docs
(hackathon_files/Databricks Asset Bundle JSON Template.pdf,
json_to_pipeline_understanding.pdf) first showed it — kept so existing mapping
JSONs don't need to be rewritten.

Design principle: Component 1's job is purely *declarative* — every field's
transformation must use one of a small, fixed, deterministic vocabulary
(`transformation.type`); `transformation.logic`/`rule` strings are a
human-readable audit trail only, never parsed to decide behavior. An
unsupported type is a loud build-time error, not a silent best-effort guess.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Transformation vocabulary
# ─────────────────────────────────────────────────────────────────────────────
# Matches the hackathon spec's Challenge 2 list verbatim ("concat, split, cast,
# conditional, lookup") plus "direct" (passthrough / unmapped-null) and the two
# date variants needed by the reference mapping examples.

T_DIRECT           = "direct"
T_CAST             = "cast"
T_CONCAT           = "concat"
T_CONDITIONAL      = "conditional"
T_DATE_FORMAT      = "date_format"
T_CONDITIONAL_DATE = "conditional_date"
T_LOOKUP           = "lookup"
T_SPLIT            = "split"

SUPPORTED_TRANSFORMATION_TYPES = {
    T_DIRECT, T_CAST, T_CONCAT, T_CONDITIONAL,
    T_DATE_FORMAT, T_CONDITIONAL_DATE, T_LOOKUP, T_SPLIT,
}

STAGE_INGEST    = "ingest"
STAGE_TRANSFORM = "transform"

_SPARK_TYPE_MAP = {
    "string":    "StringType()",
    "boolean":   "BooleanType()",
    "bool":      "BooleanType()",
    "date":      "DateType()",
    "timestamp": "TimestampType()",
    "integer":   "IntegerType()",
    "int":       "IntegerType()",
    "long":      "LongType()",
    "double":    "DoubleType()",
}

_DECIMAL_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FieldMapping:
    target_field:          str
    source_fields:         list[str]
    target_data_type:      str                    # e.g. "string", "decimal(18,2)", "boolean", "date"
    is_nullable:            bool
    default_value:          Any                    # substituted via coalesce whenever the computed value is null
    transformation_type:    str                    # one of the T_* constants
    transformation_logic:   str                    # human-readable only — NEVER parsed/executed
    transformation_params:  dict[str, Any]

    @property
    def primary_source(self) -> str:
        return self.source_fields[0] if self.source_fields else ""

    @property
    def is_compound(self) -> bool:
        return len(self.source_fields) > 1

    @property
    def spark_type_literal(self) -> str:
        """The Spark type constructor used in generated PySpark, e.g. 'DecimalType(18, 2)'."""
        m = _DECIMAL_RE.match(self.target_data_type)
        if m:
            return f"DecimalType({m.group(1)}, {m.group(2)})"
        return _SPARK_TYPE_MAP.get(self.target_data_type, "StringType()")

    @property
    def simple_type_string(self) -> str:
        """What df.schema.fields[i].dataType.simpleString() actually reports — used
        to generate the integration test's schema-conformance assertions."""
        m = _DECIMAL_RE.match(self.target_data_type)
        if m:
            return f"decimal({m.group(1)},{m.group(2)})"
        return {"integer": "int", "bool": "boolean"}.get(self.target_data_type, self.target_data_type)


@dataclass
class DataExpectation:
    name:              str
    rule:              str
    action_on_failure: str                # "drop_row" | "quarantine" | "fail_pipeline"
    parsed:            Optional[dict]      # {"field", "op", "value"} or None if rule text wasn't auto-translatable


@dataclass
class PipelineStage:
    """One hop in the pipeline chain. stage_type="ingest" (always stages[0]) is
    a straight copy from the source — no fields/expectations of its own.
    stage_type="transform" reads the PREVIOUS stage's table, applies `fields`,
    enforces `expectations`, and writes its own table."""
    name:         str
    stage_type:   str          # STAGE_INGEST | STAGE_TRANSFORM
    catalog:      str
    schema:       str
    table:        str
    format:       str
    write_mode:   str
    merge_keys:   list[str]
    fields:       list[FieldMapping]
    expectations: list[DataExpectation]

    @property
    def full_table(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

    @property
    def all_source_columns(self) -> list[str]:
        cols: list[str] = []
        for fm in self.fields:
            cols.extend(fm.source_fields)
        return list(dict.fromkeys(cols))

    @property
    def all_target_fields(self) -> list[str]:
        return [fm.target_field for fm in self.fields]

    @property
    def required_fields(self) -> list[FieldMapping]:
        return [f for f in self.fields if not f.is_nullable]


@dataclass
class PipelineSpec:
    pipeline_name: str
    description:   str
    domain:        str

    source_system:       str
    source_format:       str
    source_location:     str
    source_read_options: dict[str, str]

    stages: list[PipelineStage]     # stages[0] = ingest, stages[1:] = transforms, in chain order

    sample_inputs:           list[dict]
    sample_expected_outputs: list[dict]

    test_catalog:      str   # Unity Catalog location for Component 2's generated test fixture tables
    test_schema:       str
    test_table_prefix: str

    bundle_name:          str
    bundle_version:       str
    target_environments:  list[str]
    job_name:              str
    job_description:       str
    cluster_spark_version:  str
    cluster_node_type_id:   str
    cluster_num_workers:    int
    cluster_spark_conf:     dict[str, str]
    job_parameters:         dict[str, str]

    @property
    def target_catalog(self) -> str:
        return self.stages[0].catalog

    @property
    def ingest_stage(self) -> PipelineStage:
        return self.stages[0]

    @property
    def transform_stages(self) -> list[PipelineStage]:
        return self.stages[1:]

    @property
    def final_stage(self) -> PipelineStage:
        return self.stages[-1]

    @property
    def raw_source_columns(self) -> list[str]:
        """Source columns the ingest stage must read — inferred from what the
        FIRST transform stage declares as its inputs (the ingest stage itself
        carries no field metadata, since it's a pure passthrough)."""
        return self.stages[1].all_source_columns if len(self.stages) > 1 else []

    @property
    def test_synthetic_input_table(self) -> str:
        """Unity Catalog table Component 2 writes its full generated synthetic
        input suite (every test case, every edge case) into."""
        return f"{self.test_catalog}.{self.test_schema}.{self.test_table_prefix}_synthetic_input"

    @property
    def test_expected_output_table(self) -> str:
        """Unity Catalog table Component 2 writes its expected final-stage
        output into (one row per test case expected to reach the final
        stage) — what the integration test notebook validates against."""
        return f"{self.test_catalog}.{self.test_schema}.{self.test_table_prefix}_expected_output"

    @property
    def primary_key_field(self) -> FieldMapping:
        """The field driving row identity in the FINAL stage — merge_keys[0]
        when set (the explicit, authoritative signal), else the first
        non-nullable field, else the first field at all."""
        final = self.final_stage
        if final.merge_keys:
            for fm in final.fields:
                if fm.target_field == final.merge_keys[0]:
                    return fm
        for fm in final.fields:
            if not fm.is_nullable:
                return fm
        return final.fields[0]


# ─────────────────────────────────────────────────────────────────────────────
# Data-expectation rule mini-parser
# ─────────────────────────────────────────────────────────────────────────────
# `rule` strings in the mapping JSON are a small, fixed grammar (not arbitrary
# SQL): "<field> IS [NOT] NULL" or "<field> <op> <number>". This is a bounded,
# deterministic parser — not NLP/AI — matching every example in the hackathon's
# reference docs. A rule outside this grammar is reported (not guessed at).

_RULE_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*"
    r"(IS\s+NOT\s+NULL|IS\s+NULL|>=|<=|==|!=|=|>|<)\s*"
    r"([-+]?[0-9]*\.?[0-9]+)?\s*$",
    re.IGNORECASE,
)


def parse_expectation_rule(rule: str) -> Optional[dict]:
    m = _RULE_RE.match(rule or "")
    if not m:
        return None
    target_field, op, value = m.groups()
    op_norm = re.sub(r"\s+", " ", op.upper().strip())
    return {
        "field": target_field,
        "op": op_norm,
        "value": float(value) if value is not None else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Parser
# ─────────────────────────────────────────────────────────────────────────────

def _as_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v]
    return [str(v)]


def _split_table_name(table_name: str, default_catalog: str, default_schema: str) -> tuple[str, str, str]:
    parts = [p for p in (table_name or "").split(".") if p]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return default_catalog, parts[0], parts[1]
    if len(parts) == 1:
        return default_catalog, default_schema, parts[0]
    return default_catalog, default_schema, "unnamed_table"


def _parse_target(target_cfg: dict, default_catalog: str, default_schema: str) -> tuple[str, str, str, str, str, list[str]]:
    catalog, schema, table = _split_table_name(target_cfg.get("table_name", ""), default_catalog, default_schema)
    fmt = target_cfg.get("format", "delta")
    merge_keys = _as_list(target_cfg.get("merge_keys"))
    write_mode = target_cfg.get("write_mode") or ("merge" if merge_keys else "append")
    return catalog, schema, table, fmt, write_mode, merge_keys


def _parse_fields(mappings_raw: list[dict], verbose: bool = True) -> list[FieldMapping]:
    fields: list[FieldMapping] = []
    for row in mappings_raw:
        tgt = row.get("target_field")
        if not tgt:
            if verbose:
                print(f"  [parser] ⚠️  Skipping mapping with no target_field: {row}")
            continue

        src_fields = row.get("source_fields")
        if src_fields is None:
            src_fields = row.get("source_field")
        src_fields = _as_list(src_fields)

        transformation = row.get("transformation") or {}
        t_type = str(transformation.get("type") or T_DIRECT).strip().lower()
        if t_type not in SUPPORTED_TRANSFORMATION_TYPES:
            raise ValueError(
                f"Unsupported transformation.type={t_type!r} for target_field={tgt!r}. "
                f"Component 3 is deterministic and only compiles one of: "
                f"{sorted(SUPPORTED_TRANSFORMATION_TYPES)}. Fix the mapping JSON."
            )

        fields.append(FieldMapping(
            target_field=tgt,
            source_fields=src_fields,
            target_data_type=str(row.get("target_data_type") or "string").strip().lower(),
            is_nullable=bool(row.get("is_nullable", True)),
            default_value=row.get("default_value"),
            transformation_type=t_type,
            transformation_logic=str(transformation.get("logic") or ""),
            transformation_params=transformation.get("params") or {},
        ))
    return fields


def _parse_expectations(rows: list[dict], verbose: bool = True) -> list[DataExpectation]:
    expectations: list[DataExpectation] = []
    for row in rows:
        name   = row.get("expectation_name") or f"expectation_{len(expectations) + 1}"
        rule   = row.get("rule", "")
        action = str(row.get("action_on_failure") or "quarantine").strip().lower()
        if action not in ("drop_row", "quarantine", "fail_pipeline"):
            if verbose:
                print(f"  [parser] ⚠️  Unknown action_on_failure={action!r} for '{name}' — defaulting to 'quarantine'")
            action = "quarantine"
        parsed = parse_expectation_rule(rule)
        if parsed is None and verbose:
            print(f"  [parser] ⚠️  Rule for '{name}' ({rule!r}) is outside the supported grammar "
                  f"— it will be documented in generated code but not auto-enforced.")
        expectations.append(DataExpectation(name=name, rule=rule, action_on_failure=action, parsed=parsed))
    return expectations


def _parse_stages(raw: dict, verbose: bool) -> list[PipelineStage]:
    stages: list[PipelineStage] = []

    if "pipeline_stages" in raw:
        # ── Flexible, JSON-declared N-stage chain ──────────────────────────────
        default_catalog = "main"
        for i, stage_raw in enumerate(raw["pipeline_stages"]):
            name = stage_raw.get("stage_name") or f"stage_{i + 1}"
            stage_type = str(stage_raw.get("stage_type") or (STAGE_INGEST if i == 0 else STAGE_TRANSFORM)).strip().lower()
            if stage_type not in (STAGE_INGEST, STAGE_TRANSFORM):
                raise ValueError(f"pipeline_stages[{i}].stage_type must be 'ingest' or 'transform', got {stage_type!r}")
            target_cfg = stage_raw.get("target", {})
            catalog, schema, table, fmt, write_mode, merge_keys = _parse_target(target_cfg, default_catalog, name)
            default_catalog = catalog
            is_transform = stage_type == STAGE_TRANSFORM
            fields = _parse_fields(stage_raw.get("mappings", []), verbose) if is_transform else []
            expectations = _parse_expectations(stage_raw.get("data_expectations", []), verbose) if is_transform else []
            stages.append(PipelineStage(
                name=name, stage_type=stage_type, catalog=catalog, schema=schema, table=table,
                format=fmt, write_mode=write_mode, merge_keys=merge_keys,
                fields=fields, expectations=expectations,
            ))
        if verbose:
            print(f"  [parser] ✅ {len(stages)}-stage pipeline: {' → '.join(s.name for s in stages)}")
        return stages

    # ── Legacy shorthand: target_configurations.bronze_layer/silver_layer + top-level mappings/data_expectations ──
    tgt_cfg = raw.get("target_configurations", {})
    bronze_cfg = tgt_cfg.get("bronze_layer", {})
    silver_cfg = tgt_cfg.get("silver_layer", {})

    b_catalog, b_schema, b_table, b_fmt, b_write_mode, _ = _parse_target(bronze_cfg, "main", "bronze")
    stages.append(PipelineStage(
        name="bronze", stage_type=STAGE_INGEST, catalog=b_catalog, schema=b_schema, table=b_table,
        format=b_fmt, write_mode=b_write_mode, merge_keys=[], fields=[], expectations=[],
    ))

    s_catalog, s_schema, s_table, s_fmt, s_write_mode, s_merge_keys = _parse_target(silver_cfg, b_catalog, "silver")
    stages.append(PipelineStage(
        name="silver", stage_type=STAGE_TRANSFORM, catalog=s_catalog, schema=s_schema, table=s_table,
        format=s_fmt, write_mode=s_write_mode, merge_keys=s_merge_keys,
        fields=_parse_fields(raw.get("mappings", []), verbose),
        expectations=_parse_expectations(raw.get("data_expectations", []), verbose),
    ))
    if verbose:
        print("  [parser] ✅ legacy target_configurations shorthand auto-upgraded to a 2-stage pipeline (bronze → silver)")
    return stages


def parse_mapping_json(json_path: str, verbose: bool = True) -> PipelineSpec:
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"Mapping JSON not found: {json_path}")

    with open(json_path, "r") as f:
        raw = json.load(f)

    if verbose:
        print(f"  [parser] Loaded mapping JSON: {json_path}")

    meta = raw.get("pipeline_metadata", {})
    pipeline_name = meta.get("pipeline_name", "dpba_pipeline")
    description   = meta.get("description", "")
    domain        = meta.get("domain", "")

    src_cfg = raw.get("source_configuration", {})
    source_system       = src_cfg.get("source_system", "raw_input_system")
    source_format        = src_cfg.get("format", "csv")
    source_location       = src_cfg.get("location", f"/mnt/raw/{pipeline_name}/")
    source_read_options    = src_cfg.get("read_options") or {"header": "true", "inferSchema": "false"}

    stages = _parse_stages(raw, verbose)
    if len(stages) < 2:
        raise ValueError(
            f"{json_path}: a pipeline needs at least an ingest stage and one transform stage "
            f"(got {len(stages)}). Add a 'pipeline_stages' array or the legacy bronze_layer/silver_layer shorthand."
        )
    if not stages[-1].fields:
        raise ValueError(f"{json_path}: final stage '{stages[-1].name}' has no field mappings — nothing to produce/test.")

    # ── test_data_specifications ────────────────────────────────────────────
    test_spec = raw.get("test_data_specifications", {})
    sample_inputs           = test_spec.get("sample_inputs") or []
    sample_expected_outputs = test_spec.get("expected_outputs") or []

    uc_cfg            = test_spec.get("unity_catalog", {})
    test_catalog       = uc_cfg.get("catalog", stages[0].catalog)
    test_schema         = uc_cfg.get("schema", f"{stages[-1].schema}_test")
    test_table_prefix     = uc_cfg.get("table_prefix", pipeline_name)

    # ── bundle_metadata / workflow_specification (optional) ────────────────
    bundle_meta           = raw.get("bundle_metadata", {})
    bundle_name            = bundle_meta.get("bundle_name", f"{pipeline_name}_bundle")
    bundle_version          = bundle_meta.get("version", "1.0.0")
    target_environments      = _as_list(bundle_meta.get("target_environments")) or ["dev"]

    wf                = raw.get("workflow_specification", {})
    job_name           = wf.get("job_name", f"{pipeline_name}_workflow")
    job_description     = wf.get("description", description)
    job_clusters          = wf.get("job_clusters") or []
    cluster_cfg             = (job_clusters[0].get("new_cluster") if job_clusters else {}) or {}
    cluster_spark_version     = cluster_cfg.get("spark_version", "15.4.x-scala2.12")
    cluster_node_type_id       = cluster_cfg.get("node_type_id", "m5d.large")
    cluster_num_workers          = int(cluster_cfg.get("num_workers", 2))
    cluster_spark_conf             = cluster_cfg.get("spark_conf") or {"spark.databricks.delta.preview.enabled": "true"}
    job_parameters                   = wf.get("parameters") or {
        "catalog_name": stages[0].catalog, "schema_name": stages[-1].schema, "environment": "dev"
    }

    spec = PipelineSpec(
        pipeline_name=pipeline_name, description=description, domain=domain,
        source_system=source_system, source_format=source_format,
        source_location=source_location, source_read_options=source_read_options,
        stages=stages,
        sample_inputs=sample_inputs, sample_expected_outputs=sample_expected_outputs,
        test_catalog=test_catalog, test_schema=test_schema, test_table_prefix=test_table_prefix,
        bundle_name=bundle_name, bundle_version=bundle_version, target_environments=target_environments,
        job_name=job_name, job_description=job_description,
        cluster_spark_version=cluster_spark_version, cluster_node_type_id=cluster_node_type_id,
        cluster_num_workers=cluster_num_workers, cluster_spark_conf=cluster_spark_conf,
        job_parameters=job_parameters,
    )

    if verbose:
        total_fields = sum(len(s.fields) for s in spec.transform_stages)
        total_exps   = sum(len(s.expectations) for s in spec.transform_stages)
        print(f"  [parser] ✅ {total_fields} field mapping(s) across {len(spec.transform_stages)} transform stage(s)")
        print(f"  [parser] ✅ final stage merge_keys = {spec.final_stage.merge_keys or '(none — append mode)'}")
        print(f"  [parser] ✅ {total_exps} data expectation(s) total")
        print(f"  [parser] ✅ {len(spec.sample_inputs)} seed sample input(s) from test_data_specifications")
        print(f"  [parser] ✅ test fixtures → {spec.test_synthetic_input_table} / {spec.test_expected_output_table}")

    return spec


def print_spec_table(spec: PipelineSpec) -> None:
    """Pretty-print the parsed mapping as a human-readable table."""
    print(f"  Pipeline chain: {' → '.join(s.name for s in spec.stages)}")
    for stage in spec.transform_stages:
        print(f"\n  ── Stage '{stage.name}'  (writes {stage.full_table}, merge_keys={stage.merge_keys or 'none'}) ──")
        hdr = f"  {'Target Field':<22} {'Source Field(s)':<24} {'Type':<14} {'Null':<6} {'Transform':<14} Logic"
        print(hdr)
        print("  " + "─" * 110)
        for fm in stage.fields:
            src_display = " + ".join(fm.source_fields) if fm.source_fields else "(none)"
            print(
                f"  {fm.target_field:<22} {src_display:<24} {fm.target_data_type:<14} "
                f"{str(fm.is_nullable):<6} {fm.transformation_type:<14} {fm.transformation_logic}"
            )
        if stage.expectations:
            print()
            print("  Data Expectations:")
            for exp in stage.expectations:
                status = "" if exp.parsed else "  ⚠️  not auto-enforceable"
                print(f"    [{exp.action_on_failure:<13}] {exp.name}: {exp.rule}{status}")
