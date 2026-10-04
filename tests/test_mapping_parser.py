"""Regression tests for mapping_parser.py: schema parsing, the expectation
rule grammar, primary-key resolution, and type rendering."""

from __future__ import annotations

import json
import os

import pytest

from mapping_parser import (
    parse_mapping_json, parse_expectation_rule, FieldMapping,
    T_CAST,
)


class TestLegacyShorthandAutoUpgrade:
    def test_becomes_a_two_stage_chain(self, customer_spec):
        assert [s.name for s in customer_spec.stages] == ["bronze", "silver"]
        assert customer_spec.stages[0].stage_type == "ingest"
        assert customer_spec.stages[1].stage_type == "transform"

    def test_ingest_stage_has_no_fields_of_its_own(self, customer_spec):
        assert customer_spec.ingest_stage.fields == []

    def test_final_stage_has_field_mappings(self, customer_spec):
        assert len(customer_spec.final_stage.fields) > 0


class TestFlexibleStageChain:
    def test_three_stage_medallion_parses(self, medallion_spec):
        assert [s.name for s in medallion_spec.stages] == ["bronze", "silver", "gold"]
        assert len(medallion_spec.transform_stages) == 2
        assert medallion_spec.final_stage.name == "gold"

    def test_gold_stage_reads_silvers_target_columns(self, medallion_spec):
        gold = medallion_spec.final_stage
        silver_targets = {fm.target_field for fm in medallion_spec.stages[1].fields}
        for fm in gold.fields:
            for src in fm.source_fields:
                assert src in silver_targets, (
                    f"gold field {fm.target_field!r} reads {src!r}, which isn't one of "
                    f"silver's own target fields — chain convention broken"
                )

    def test_every_sample_has_at_least_two_stages(self, sample_spec):
        assert len(sample_spec.stages) >= 2
        assert sample_spec.final_stage.fields, "final stage must have field mappings"


class TestExpectationRuleParser:
    @pytest.mark.parametrize("rule,expected", [
        ("customer_id IS NOT NULL", {"field": "customer_id", "op": "IS NOT NULL", "value": None}),
        ("account_status IS NULL", {"field": "account_status", "op": "IS NULL", "value": None}),
        ("account_balance >= 0", {"field": "account_balance", "op": ">=", "value": 0.0}),
        ("x <= 100.5", {"field": "x", "op": "<=", "value": 100.5}),
        ("x != 5", {"field": "x", "op": "!=", "value": 5.0}),
        ("x == 5", {"field": "x", "op": "==", "value": 5.0}),
        ("x > -3.2", {"field": "x", "op": ">", "value": -3.2}),
    ])
    def test_parses_supported_grammar(self, rule, expected):
        assert parse_expectation_rule(rule) == expected

    @pytest.mark.parametrize("rule", [
        "account_balance BETWEEN 0 AND 100",
        "customer_name LIKE '%smith%'",
        "x >= 0 AND y <= 10",
        "",
        "garbage !!!",
    ])
    def test_rejects_unsupported_grammar_instead_of_guessing(self, rule):
        assert parse_expectation_rule(rule) is None


class TestPrimaryKeyResolution:
    def test_prefers_merge_keys_over_heuristics(self, customer_spec):
        pk = customer_spec.primary_key_field
        assert customer_spec.final_stage.merge_keys
        assert pk.target_field == customer_spec.final_stage.merge_keys[0]

    def test_every_sample_resolves_a_primary_key(self, sample_spec):
        pk = sample_spec.primary_key_field
        assert pk is not None
        assert pk.target_field in sample_spec.final_stage.all_target_fields


class TestFieldMappingTypeRendering:
    @staticmethod
    def _fm(target_data_type: str) -> FieldMapping:
        return FieldMapping(
            target_field="x", source_fields=["y"], target_data_type=target_data_type,
            is_nullable=True, default_value=None, transformation_type=T_CAST,
            transformation_logic="", transformation_params={},
        )

    @pytest.mark.parametrize("dtype,spark_literal,simple_str", [
        ("string", "StringType()", "string"),
        ("boolean", "BooleanType()", "boolean"),
        ("date", "DateType()", "date"),
        ("integer", "IntegerType()", "int"),
        ("decimal(18,2)", "DecimalType(18, 2)", "decimal(18,2)"),
        ("decimal(10,0)", "DecimalType(10, 0)", "decimal(10,0)"),
    ])
    def test_type_rendering_round_trips(self, dtype, spark_literal, simple_str):
        fm = self._fm(dtype)
        assert fm.spark_type_literal == spark_literal
        assert fm.simple_type_string == simple_str


class TestUnsupportedTransformationType:
    def test_raises_a_clear_error_instead_of_guessing(self, tmp_path):
        bad_mapping = {
            "pipeline_metadata": {"pipeline_name": "bad"},
            "target_configurations": {
                "bronze_layer": {"table_name": "main.bronze.x"},
                "silver_layer": {"table_name": "main.silver.x"},
            },
            "mappings": [
                {"target_field": "x", "source_fields": ["y"],
                 "transformation": {"type": "not_a_real_type"}}
            ],
        }
        p = tmp_path / "bad.json"
        p.write_text(json.dumps(bad_mapping))
        with pytest.raises(ValueError, match="Unsupported transformation.type"):
            parse_mapping_json(str(p), verbose=False)


class TestUnityCatalogFixtureTables:
    def test_default_table_names_derive_from_pipeline_and_final_stage(self, customer_spec):
        assert customer_spec.test_synthetic_input_table.endswith("_synthetic_input")
        assert customer_spec.test_expected_output_table.endswith("_expected_output")
        assert customer_spec.test_catalog == customer_spec.stages[0].catalog
        assert customer_spec.test_schema == f"{customer_spec.final_stage.schema}_test"

    def test_every_sample_resolves_fixture_table_names(self, sample_spec):
        assert sample_spec.test_synthetic_input_table.count(".") == 2
        assert sample_spec.test_expected_output_table.count(".") == 2
