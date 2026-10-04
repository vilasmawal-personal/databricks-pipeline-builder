"""
Full end-to-end regression: runs Component 2 then Component 3 exactly the
way dpba_runner.py does, against every committed sample spec, into a tmp_path
output directory — and checks the whole on-disk artifact set, not just
in-memory objects. This is the closest thing in this suite to actually
running `dpba_runner.py` and is what would have caught the original PK
off-by-one bug (it was originally found by diffing these exact two files).
"""

from __future__ import annotations

import ast
import csv

import yaml

from mapping_parser import parse_mapping_json
from component2_data_generator import run as run_component2
from conftest import raw_pk_source
from component3_pipeline_builder import run as run_component3


class TestFullPipelineGeneration:
    def test_generates_a_complete_valid_bundle_for_every_sample(self, sample_spec_path, tmp_path):
        spec = parse_mapping_json(sample_spec_path, verbose=False)
        test_data_dir = tmp_path / "test_data"
        pipeline_dir = tmp_path / "pipeline"

        test_cases = run_component2(spec, output_dir=str(test_data_dir), skip_hitl=True)
        assert test_cases, "Component 2 produced no test cases"

        artifacts = run_component3(
            spec, output_dir=str(pipeline_dir), test_cases=test_cases, skip_hitl=True
        )
        assert artifacts, "Component 3 produced no artifacts"

        notebooks_dir = pipeline_dir / "notebooks"
        py_files = sorted(notebooks_dir.glob("*.py"))
        # fixtures + ingest + one per transform stage + integration tests
        expected_count = 2 + len(spec.transform_stages) + 1
        assert len(py_files) == expected_count, (
            f"expected {expected_count} notebooks for a {len(spec.stages)}-stage "
            f"pipeline, found {[f.name for f in py_files]}"
        )

        for f in py_files:
            try:
                ast.parse(f.read_text())
            except SyntaxError as e:
                raise AssertionError(f"{f.name} is invalid Python: {e}")

        yaml.safe_load((pipeline_dir / "databricks.yml").read_text())
        yaml.safe_load((pipeline_dir / "resources" / "pipeline_job.yml").read_text())

        for name in ("synthetic_input.csv", "expected_output.csv", "test_cases.json"):
            assert (test_data_dir / name).exists(), f"missing {name}"

    def test_pk_consistency_in_the_actual_written_csvs(self, sample_spec_path, tmp_path):
        """The exact check that originally caught the PK off-by-one bug,
        run against real files on disk rather than in-memory objects."""
        spec = parse_mapping_json(sample_spec_path, verbose=False)
        test_data_dir = tmp_path / "test_data"
        run_component2(spec, output_dir=str(test_data_dir), skip_hitl=True)

        with open(test_data_dir / "synthetic_input.csv") as f:
            input_rows = {r["_tc_id"]: r for r in csv.DictReader(f)}
        with open(test_data_dir / "expected_output.csv") as f:
            expected_rows = {r["_tc_id"]: r for r in csv.DictReader(f)}

        pk_target = spec.primary_key_field.target_field
        pk_source = raw_pk_source(spec)
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

        assert not mismatches, f"PK mismatches in generated CSVs: {mismatches}"

    def test_job_yaml_notebook_paths_match_files_actually_written(self, sample_spec_path, tmp_path):
        """Catches drift between what the job YAML references and what
        Component 3 actually wrote to disk — a job that points at a
        notebook path nothing created would fail at deploy/run time."""
        spec = parse_mapping_json(sample_spec_path, verbose=False)
        test_data_dir = tmp_path / "test_data"
        pipeline_dir = tmp_path / "pipeline"

        test_cases = run_component2(spec, output_dir=str(test_data_dir), skip_hitl=True)
        run_component3(spec, output_dir=str(pipeline_dir), test_cases=test_cases, skip_hitl=True)

        written_names = {f.stem for f in (pipeline_dir / "notebooks").glob("*.py")}

        job_doc = yaml.safe_load((pipeline_dir / "resources" / "pipeline_job.yml").read_text())
        job = next(iter(job_doc["resources"]["jobs"].values()))
        for task in job["tasks"]:
            notebook_path = task["notebook_task"]["notebook_path"]
            referenced_name = notebook_path.rsplit("/", 1)[-1]
            assert referenced_name in written_names, (
                f"task {task['task_key']!r} references notebook {referenced_name!r}, "
                f"which was never written to {pipeline_dir / 'notebooks'}"
            )
