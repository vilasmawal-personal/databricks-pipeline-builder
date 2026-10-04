"""
Regression tests for component3_pipeline_builder.py: snippet codegen per
transformation type, and — most importantly — that every generated artifact
(notebooks, job YAML, bundle YAML) is actually syntactically valid, across
every stage-chain shape. Several real bugs (an f-string reindent bug that
produced invalid Python/YAML; a hardcoded schema that never actually
matched the generated code) were only ever caught by checks exactly like
these, not by eyeballing the templates.
"""

from __future__ import annotations

import ast

import pytest
import yaml

from dpba.models.pipeline_spec import FieldMapping
from mapping_parser import T_CAST, T_CONCAT, T_CONDITIONAL, T_LOOKUP
from component2_data_generator import DataGenerator, generate_load_fixtures_notebook
from component3_pipeline_builder import (
    SnippetLibrary, _apply_default, _get_snippet,
    generate_ingest_notebook, generate_transform_notebook, generate_integration_test_notebook,
    generate_databricks_yml, generate_job_yml,
)


def _fm(**overrides) -> FieldMapping:
    defaults = dict(
        target_field="x", source_fields=["y"], target_data_type="string",
        is_nullable=True, default_value=None, transformation_type=T_CAST,
        transformation_logic="", transformation_params={},
    )
    defaults.update(overrides)
    from dpba.models.pipeline_spec import TransformationDef
    return FieldMapping(
        target_field=defaults["target_field"],
        source_fields=defaults["source_fields"],
        target_type=defaults["target_data_type"],
        is_nullable=defaults["is_nullable"],
        default_value=defaults["default_value"],
        transformation=TransformationDef(
            type=defaults["transformation_type"],
            params=defaults["transformation_params"]
        )
    )


class TestSnippetLibrary:
    def test_cast_trims_string_targets(self):
        snippet = SnippetLibrary.cast(_fm(target_data_type="string"))
        assert "F.trim" in snippet
        assert 'F.col("y")' in snippet

    def test_cast_does_not_trim_non_string_targets(self):
        snippet = SnippetLibrary.cast(_fm(target_data_type="integer"))
        assert "F.trim" not in snippet

    def test_conditional_uses_the_declared_divisor(self):
        fm = _fm(transformation_type=T_CONDITIONAL, target_data_type="decimal(18,2)",
                 transformation_params={"divisor": 100})
        assert "F.lit(100)" in SnippetLibrary.conditional(fm)

    def test_concat_all_null_check_covers_every_source_field(self):
        fm = _fm(transformation_type=T_CONCAT, source_fields=["a", "b", "c"])
        snippet = SnippetLibrary.concat(fm)
        for src in ["a", "b", "c"]:
            assert f'F.col("{src}").isNull()' in snippet

    def test_lookup_renders_every_value_map_entry(self):
        fm = _fm(transformation_type=T_LOOKUP, target_data_type="boolean",
                 transformation_params={"value_map": {"Y": True, "N": False}})
        snippet = SnippetLibrary.lookup(fm)
        assert '== "Y"' in snippet and '== "N"' in snippet
        assert "F.lit(True)" in snippet and "F.lit(False)" in snippet

    def test_direct_with_no_source_fields_is_always_null(self):
        fm = _fm(transformation_type="direct", source_fields=[])
        snippet = SnippetLibrary.direct(fm)
        assert "F.lit(None)" in snippet


class TestUniversalDefaultHandling:
    def test_wraps_with_coalesce_when_default_is_set(self):
        fm = _fm(default_value="UNKNOWN", target_data_type="string")
        wrapped = _apply_default(fm, 'df = df.withColumn("x", F.lit(None))')
        assert "F.coalesce" in wrapped
        assert '"UNKNOWN"' in wrapped

    def test_is_a_noop_when_no_default(self):
        fm = _fm(default_value=None)
        snippet = 'df = df.withColumn("x", F.lit(None))'
        assert _apply_default(fm, snippet) == snippet

    def test_renders_numeric_default_as_a_bare_literal(self):
        fm = _fm(default_value=0.0, target_data_type="decimal(18,2)")
        wrapped = _apply_default(fm, 'df = df.withColumn("x", F.lit(None))')
        assert "F.lit(0.0)" in wrapped


class TestDispatchRaisesOnUnsupportedType:
    def test_get_snippet_raises_for_unknown_type(self):
        with pytest.raises(ValueError):
            fm = _fm(transformation_type="not_a_real_type")
            _get_snippet(fm)


class TestGeneratedArtifactsAreSyntacticallyValid:
    """The highest-value regression check in this suite: every generated
    notebook must be valid Python, every generated YAML file must be valid
    YAML — for every stage-chain shape committed to sample_specs/."""

    def test_all_notebooks_parse_as_valid_python(self, sample_spec):
        tcs = DataGenerator(sample_spec).generate()
        notebooks = {
            "00_load_test_fixtures": generate_load_fixtures_notebook(sample_spec, tcs),
            f"01_ingest_{sample_spec.ingest_stage.name}": generate_ingest_notebook(sample_spec),
        }
        for i, stage in enumerate(sample_spec.transform_stages, start=1):
            notebooks[f"0{i+1}_transform_{stage.name}"] = generate_transform_notebook(sample_spec, i)
        notebooks["integration_tests"] = generate_integration_test_notebook(sample_spec)

        for name, content in notebooks.items():
            try:
                ast.parse(content)
            except SyntaxError as e:
                raise AssertionError(f"{name} is invalid Python: {e}\n\n--- content ---\n{content}")

    def test_databricks_yml_is_valid_yaml(self, sample_spec):
        yaml.safe_load(generate_databricks_yml(sample_spec))

    def test_job_yml_is_valid_yaml(self, sample_spec):
        yaml.safe_load(generate_job_yml(sample_spec))


class TestJobDAG:
    @staticmethod
    def _job(spec):
        doc = yaml.safe_load(generate_job_yml(spec))
        return next(iter(doc["resources"]["jobs"].values()))

    def test_load_test_fixtures_has_no_dependencies(self, sample_spec):
        job = self._job(sample_spec)
        tasks = {t["task_key"]: t for t in job["tasks"]}
        assert not tasks["load_test_fixtures"].get("depends_on")

    def test_integration_tests_depends_on_fixtures_and_final_transform(self, sample_spec):
        job = self._job(sample_spec)
        tasks = {t["task_key"]: t for t in job["tasks"]}
        deps = {d["task_key"] for d in tasks["run_integration_tests"]["depends_on"]}
        assert "load_test_fixtures" in deps
        assert f"transform_to_{sample_spec.final_stage.name}" in deps

    def test_stage_chain_is_fully_connected_in_order(self, sample_spec):
        job = self._job(sample_spec)
        tasks = {t["task_key"]: t for t in job["tasks"]}
        prev_key = f"ingest_to_{sample_spec.ingest_stage.name}"
        assert prev_key in tasks
        for stage in sample_spec.transform_stages:
            key = f"transform_to_{stage.name}"
            assert key in tasks, f"missing task for stage {stage.name!r}"
            deps = {d["task_key"] for d in tasks[key].get("depends_on", [])}
            assert prev_key in deps, f"{key} should depend on {prev_key}"
            prev_key = key

    def test_task_count_matches_stage_count(self, sample_spec):
        job = self._job(sample_spec)
        # 1 fixtures + 1 ingest + len(transform_stages) + 1 integration test
        expected = 1 + 1 + len(sample_spec.transform_stages) + 1
        assert len(job["tasks"]) == expected

    def test_transform_write_modes_follow_mapping(self, sample_spec):
        job = self._job(sample_spec)
        tasks = {t["task_key"]: t for t in job["tasks"]}
        for stage in sample_spec.transform_stages:
            params = tasks[f"transform_to_{stage.name}"]["notebook_task"]["base_parameters"]
            assert params["write_mode"] == stage.write_mode


class TestComputeRouting:
    """least-compute regression: the two lightweight utility tasks must
    never end up sharing the heavier autoscaling cluster."""

    @staticmethod
    def _job(spec):
        doc = yaml.safe_load(generate_job_yml(spec))
        return next(iter(doc["resources"]["jobs"].values()))

    def test_utility_tasks_use_the_single_node_cluster(self, sample_spec):
        job = self._job(sample_spec)
        tasks = {t["task_key"]: t for t in job["tasks"]}
        assert tasks["load_test_fixtures"]["job_cluster_key"] == "test_utility_cluster"
        assert tasks["run_integration_tests"]["job_cluster_key"] == "test_utility_cluster"

    def test_single_node_cluster_has_zero_workers(self, sample_spec):
        job = self._job(sample_spec)
        clusters = {c["job_cluster_key"]: c for c in job["job_clusters"]}
        assert clusters["test_utility_cluster"]["new_cluster"]["num_workers"] == 0

    def test_ingest_and_transform_tasks_use_the_shared_cluster(self, sample_spec):
        job = self._job(sample_spec)
        for t in job["tasks"]:
            if t["task_key"].startswith(("ingest_to_", "transform_to_")):
                assert t["job_cluster_key"] == "pipeline_shared_cluster"


class TestMultiEnvironmentCatalogSubstitution:
    """Regression test: every base_parameter referencing a table must use
    ${var.catalog} (so `databricks bundle deploy -t prod` with an overridden
    catalog variable actually repoints every task) UNLESS the mapping JSON
    explicitly set a different, standalone catalog for test fixtures via
    test_data_specifications.unity_catalog.catalog — a deliberate choice
    that must be respected as a literal, not silently overridden per target."""

    @staticmethod
    def _base_params(spec, task_key: str) -> dict:
        import yaml as _yaml
        from component3_pipeline_builder import generate_job_yml
        doc = _yaml.safe_load(generate_job_yml(spec))
        job = next(iter(doc["resources"]["jobs"].values()))
        tasks = {t["task_key"]: t for t in job["tasks"]}
        return tasks[task_key]["notebook_task"]["base_parameters"]

    def test_fixture_tables_use_var_catalog_when_catalog_matches_pipeline(self, sample_spec):
        assert sample_spec.test_catalog == sample_spec.target_catalog, (
            "every shipped sample defaults test fixtures to the pipeline's own "
            "catalog — if this assumption ever changes, see the 'else' branch below"
        )
        params = self._base_params(sample_spec, "load_test_fixtures")
        assert params["synthetic_input_table"].startswith("${var.catalog}.")
        assert params["expected_output_table"].startswith("${var.catalog}.")

        test_params = self._base_params(sample_spec, "run_integration_tests")
        assert test_params["synthetic_input_table"].startswith("${var.catalog}.")
        assert test_params["expected_output_table"].startswith("${var.catalog}.")

    def test_pipeline_tables_always_use_var_catalog(self, sample_spec):
        ingest_key = f"ingest_to_{sample_spec.ingest_stage.name}"
        params = self._base_params(sample_spec, ingest_key)
        assert params[f"{sample_spec.ingest_stage.name}_table"].startswith("${var.catalog}.")

    def test_explicit_fixture_catalog_override_is_kept_literal(self, tmp_path):
        """When unity_catalog.catalog differs from the pipeline's own catalog,
        it's a deliberate separation (e.g. a shared cross-pipeline test
        catalog) — ${var.catalog} must NOT silently repoint it."""
        import json
        from mapping_parser import parse_mapping_json

        mapping = {
            "pipeline_metadata": {"pipeline_name": "x"},
            "target_configurations": {
                "bronze_layer": {"table_name": "main.bronze.x"},
                "silver_layer": {"table_name": "main.silver.x", "merge_keys": ["id"]},
            },
            "mappings": [
                {"target_field": "id", "source_fields": ["id_raw"], "is_nullable": False,
                 "transformation": {"type": "cast"}}
            ],
            "test_data_specifications": {
                "unity_catalog": {"catalog": "shared_test_catalog"}
            },
        }
        p = tmp_path / "x.json"
        p.write_text(json.dumps(mapping))
        spec = parse_mapping_json(str(p), verbose=False)
        assert spec.test_catalog == "shared_test_catalog"
        assert spec.target_catalog == "main"

        params = self._base_params(spec, "load_test_fixtures")
        assert params["synthetic_input_table"].startswith("shared_test_catalog.")
        assert "${var.catalog}" not in params["synthetic_input_table"]


class TestMergeAndSchemaCreation:
    def test_merge_keys_stage_generates_a_real_delta_merge(self, customer_spec):
        content = generate_transform_notebook(customer_spec, 1)
        assert "DeltaTable.forName" in content
        assert ".merge(" in content
        assert "whenMatchedUpdateAll" in content
        assert "whenNotMatchedInsertAll" in content

    def test_every_write_point_creates_its_schema_first(self, sample_spec):
        tcs = DataGenerator(sample_spec).generate()
        assert "CREATE SCHEMA IF NOT EXISTS" in generate_ingest_notebook(sample_spec)
        for i in range(1, len(sample_spec.transform_stages) + 1):
            assert "CREATE SCHEMA IF NOT EXISTS" in generate_transform_notebook(sample_spec, i)
        assert "CREATE SCHEMA IF NOT EXISTS" in generate_load_fixtures_notebook(sample_spec, tcs)


class TestSchemaConformanceAssertionsMatchCodegen:
    """The integration-test notebook's EXPECTED_SCHEMA must describe exactly
    the types the transform notebook's snippets actually produce — these two
    must never be allowed to drift apart (they did once, hardcoded)."""

    def test_expected_schema_covers_every_final_stage_field(self, sample_spec):
        test_notebook = generate_integration_test_notebook(sample_spec)
        for fm in sample_spec.final_stage.fields:
            assert f'"{fm.target_field}": "{fm.simple_type_string}"' in test_notebook
