"""
Regression tests for component2_data_generator.py.

The two test classes at the top of this file exist specifically to lock in
bugs that were found and fixed by hand during development, so they can
never silently come back:

  - TestPrimaryKeyConsistency: the PK off-by-one bug (a generated test
    case's input row and expected row disagreed on the primary-key value,
    because the id was computed twice from two different counter states).

  - TestDispositionResolution: the cross-expectation bug (a row built to
    exercise one field's boundary value — e.g. a negative account_balance —
    incidentally violated an UNRELATED expectation on the same field, and
    was shipped mislabeled as 'silver' when the real pipeline would have
    quarantined or dropped it).
"""

from __future__ import annotations

import csv

from component2_data_generator import (
    DataGenerator, _compute_raw_expected, _rule_satisfied,
    write_synthetic_input_csv, write_expected_output_csv,
)
from mapping_parser import FieldMapping, T_CONDITIONAL, T_CONCAT, T_LOOKUP, T_DATE_FORMAT

from conftest import raw_pk_source


class TestPrimaryKeyConsistency:
    """Regression test for the historical PK off-by-one bug."""

    def test_pk_matches_between_input_and_expected_rows(self, sample_spec):
        tcs = DataGenerator(sample_spec).generate()
        pk_target = sample_spec.primary_key_field.target_field
        pk_source = raw_pk_source(sample_spec)

        mismatches = []
        for tc in tcs:
            if tc.expected_disposition != "silver":
                continue
            raw_pk = tc.input_row.get(pk_source)
            expected_pk = tc.expected_row.get(pk_target)
            if raw_pk is None or expected_pk is None:
                continue
            if str(raw_pk).strip() != str(expected_pk).strip():
                mismatches.append((tc.tc_id, tc.scenario, raw_pk, expected_pk))

        assert not mismatches, (
            f"PK mismatch between a silver row's input and expected row "
            f"(the exact shape of the historical off-by-one bug): {mismatches}"
        )

    def test_every_silver_row_has_a_distinct_pk_except_the_duplicate_pair(self, sample_spec):
        tcs = DataGenerator(sample_spec).generate()
        pk_target = sample_spec.primary_key_field.target_field
        silver = [tc for tc in tcs if tc.expected_disposition == "silver"]

        by_pk: dict[str, list[str]] = {}
        for tc in silver:
            pk = tc.expected_row.get(pk_target)
            by_pk.setdefault(pk, []).append(tc.tc_id)

        collisions = {pk: ids for pk, ids in by_pk.items() if len(ids) > 1}
        # exactly one PK value is intentionally shared — the dedicated duplicate-pair test
        assert len(collisions) <= 1, f"Unexpected PK collisions across unrelated rows: {collisions}"
        if collisions:
            ((_, ids),) = collisions.items()
            assert len(ids) == 2, f"Duplicate-PK group should have exactly 2 rows, got: {ids}"


class TestDispositionResolution:
    """Regression test for the cross-expectation disposition bug.

    Only the FINAL stage's own expectations are checked here: tc.expected_row
    is shaped like the final stage's output columns, so an intermediate
    stage's expectation (e.g. silver's 'valid_account_status' in a
    bronze->silver->gold chain where gold renamed/dropped that field) isn't
    meaningfully checkable against it — that stage's own disposition
    resolution is exercised instead by TestPrimaryKeyConsistency and the
    end-to-end suite, which run the real chain-aware generator unmodified."""

    def test_no_silver_row_violates_a_final_stage_quarantine_or_drop_expectation(self, sample_spec):
        tcs = DataGenerator(sample_spec).generate()
        for exp in sample_spec.final_stage.expectations:
            if not exp.parsed or exp.action_on_failure == "fail_pipeline":
                continue
            for tc in tcs:
                if tc.expected_disposition != "silver":
                    continue
                actual = tc.expected_row.get(exp.parsed["field"])
                assert _rule_satisfied(actual, exp.parsed["op"], exp.parsed["value"]), (
                    f"{tc.tc_id} ({tc.scenario!r}) is tagged 'silver' but its computed "
                    f"value violates '{exp.rule}' ({exp.name}) — the real pipeline would "
                    f"never let this row reach the final stage as-is"
                )

    def test_no_generated_row_would_trip_a_final_stage_fail_pipeline_expectation(self, sample_spec):
        """A row violating a fail_pipeline rule would raise inside the transform
        notebook and abort the ENTIRE integration-test batch — generate()'s
        exclusion filter must have caught it before it ever got here."""
        tcs = DataGenerator(sample_spec).generate()
        for exp in sample_spec.final_stage.expectations:
            if not exp.parsed or exp.action_on_failure != "fail_pipeline":
                continue
            for tc in tcs:
                if tc.expected_disposition != "silver":
                    continue
                actual = tc.expected_row.get(exp.parsed["field"])
                assert _rule_satisfied(actual, exp.parsed["op"], exp.parsed["value"]), (
                    f"{tc.tc_id} would trip fail_pipeline expectation '{exp.name}' and "
                    f"crash the whole integration-test batch if deployed"
                )


class TestExpectedValueMirror:
    """_compute_raw_expected is the single source of truth shared with
    Component 3's SnippetLibrary — these spot-check it against hand-computed
    values so the two can't silently drift apart."""

    def test_conditional_divides_by_the_declared_divisor(self):
        fm = FieldMapping(
            target_field="account_balance", source_fields=["balance_cents"],
            target_data_type="decimal(18,2)", is_nullable=True, default_value=0.0,
            transformation_type=T_CONDITIONAL, transformation_logic="",
            transformation_params={"divisor": 100},
        )
        assert _compute_raw_expected(fm, {"balance_cents": "125050"}) == 1250.50
        assert _compute_raw_expected(fm, {"balance_cents": "-500"}) == -5.0
        assert _compute_raw_expected(fm, {"balance_cents": "abc"}) is None
        assert _compute_raw_expected(fm, {"balance_cents": None}) is None

    def test_concat_matches_concat_ws_null_skipping_semantics(self):
        fm = FieldMapping(
            target_field="full_name", source_fields=["first_name", "last_name"],
            target_data_type="string", is_nullable=True, default_value=None,
            transformation_type=T_CONCAT, transformation_logic="",
            transformation_params={"separator": " "},
        )
        assert _compute_raw_expected(fm, {"first_name": "Jane", "last_name": "Doe"}) == "Jane Doe"
        assert _compute_raw_expected(fm, {"first_name": None, "last_name": "Doe"}) == "Doe"
        assert _compute_raw_expected(fm, {"first_name": None, "last_name": None}) is None

    def test_lookup_is_case_insensitive_by_default(self):
        fm = FieldMapping(
            target_field="account_status", source_fields=["acct_status_flag"],
            target_data_type="boolean", is_nullable=False, default_value=None,
            transformation_type=T_LOOKUP, transformation_logic="",
            transformation_params={"value_map": {"Y": True, "N": False}},
        )
        assert _compute_raw_expected(fm, {"acct_status_flag": "y"}) is True
        assert _compute_raw_expected(fm, {"acct_status_flag": "N"}) is False
        assert _compute_raw_expected(fm, {"acct_status_flag": "X"}) is None

    def test_date_format_tries_each_source_format_in_order(self):
        fm = FieldMapping(
            target_field="signup_date", source_fields=["signup_dt"],
            target_data_type="date", is_nullable=True, default_value=None,
            transformation_type=T_DATE_FORMAT, transformation_logic="",
            transformation_params={"source_formats": ["yyyyMMdd", "yyyy-MM-dd"]},
        )
        assert _compute_raw_expected(fm, {"signup_dt": "20230115"}) == "2023-01-15"
        assert _compute_raw_expected(fm, {"signup_dt": "2023-06-20"}) == "2023-06-20"
        assert _compute_raw_expected(fm, {"signup_dt": "not-a-date"}) is None


class TestGeneratedCaseSetSanity:
    def test_every_test_case_has_a_unique_tc_id(self, sample_spec):
        tcs = DataGenerator(sample_spec).generate()
        ids = [tc.tc_id for tc in tcs]
        assert len(ids) == len(set(ids))

    def test_includes_happy_path_and_at_least_one_of_each_disposition_or_only_silver(self, sample_spec):
        tcs = DataGenerator(sample_spec).generate()
        edge_cases = {tc.edge_case for tc in tcs}
        assert "happy_path" in edge_cases
        dispositions = {tc.expected_disposition for tc in tcs}
        assert "silver" in dispositions
        assert dispositions <= {"silver", "quarantine", "dropped"}

    def test_quarantine_and_dropped_rows_have_empty_expected_row(self, sample_spec):
        tcs = DataGenerator(sample_spec).generate()
        for tc in tcs:
            if tc.expected_disposition in ("quarantine", "dropped"):
                assert tc.expected_row == {}, (
                    f"{tc.tc_id} is {tc.expected_disposition!r} but still carries an "
                    f"expected_row — it should never reach the final stage"
                )


class TestWriters:
    def test_expected_output_csv_contains_only_silver_rows(self, sample_spec, tmp_path):
        tcs = DataGenerator(sample_spec).generate()
        path = tmp_path / "expected_output.csv"
        write_expected_output_csv(tcs, str(path))

        with open(path) as f:
            rows = list(csv.DictReader(f))

        silver_ids = {tc.tc_id for tc in tcs if tc.expected_disposition == "silver"}
        assert {r["_tc_id"] for r in rows} == silver_ids

    def test_synthetic_input_csv_contains_every_test_case(self, sample_spec, tmp_path):
        tcs = DataGenerator(sample_spec).generate()
        path = tmp_path / "synthetic_input.csv"
        write_synthetic_input_csv(tcs, str(path))

        with open(path) as f:
            rows = list(csv.DictReader(f))

        assert {r["_tc_id"] for r in rows} == {tc.tc_id for tc in tcs}

    def test_pk_consistency_via_the_written_csvs(self, sample_spec, tmp_path):
        """Same guarantee as TestPrimaryKeyConsistency, exercised through the
        actual CSV writers instead of the in-memory TestCase objects — this
        is exactly the check that originally caught the off-by-one bug by
        hand, now automated."""
        tcs = DataGenerator(sample_spec).generate()
        write_synthetic_input_csv(tcs, str(tmp_path / "synthetic_input.csv"))
        write_expected_output_csv(tcs, str(tmp_path / "expected_output.csv"))

        with open(tmp_path / "synthetic_input.csv") as f:
            input_rows = {r["_tc_id"]: r for r in csv.DictReader(f)}
        with open(tmp_path / "expected_output.csv") as f:
            expected_rows = {r["_tc_id"]: r for r in csv.DictReader(f)}

        pk_target = sample_spec.primary_key_field.target_field
        pk_source = raw_pk_source(sample_spec)
        mismatches = []
        for tc_id, inp in input_rows.items():
            if inp["_expected_disposition"] != "silver":
                continue
            exp = expected_rows.get(tc_id)
            if exp is None:
                mismatches.append((tc_id, "missing from expected_output.csv"))
                continue
            raw_pk = (inp.get(pk_source) or "").strip()
            expected_pk = (exp.get(pk_target) or "").strip()
            if raw_pk != expected_pk:
                mismatches.append((tc_id, raw_pk, expected_pk))

        assert not mismatches, f"CSV-level PK mismatches: {mismatches}"
