"""
DPBA — Shared: Mapping Parser
==============================
Parses the canonical "universal JSON" structured mapping document (the
declarative hand-off contract between Component 1 — the AI Consultant Agent —
and Components 2/3) into a PipelineSpec Pydantic model.

This uses the new Canonical IR (Gate 2).
It also provides backward-compatibility normalization for legacy formats.
"""

import json
import os
import re
from typing import Any, Dict, List, Optional
from dpba.models.pipeline_spec import (
    PipelineSpec, PipelineMetadata, SourceDefinition, PipelineStage, StageOperations,
    OutputDefinition, FieldMapping, TransformationDef, DataExpectation, ConditionDef,
    TestSpecification, EnvironmentSpec, ClusterDef, ProvenanceMetadata
)

# ─────────────────────────────────────────────────────────────────────────────
# Transformation vocabulary
# ─────────────────────────────────────────────────────────────────────────────
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


def _split_table_name(table_name: str, default_catalog: str, default_schema: str) -> tuple:
    parts = [p for p in (table_name or "").split(".") if p]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return default_catalog, parts[0], parts[1]
    if len(parts) == 1:
        return default_catalog, default_schema, parts[0]
    return default_catalog, default_schema, "unnamed_table"

def _parse_legacy_rule(rule: str) -> ConditionDef:
    """Parse legacy SQL-string data expectation rules into the new AST condition def."""
    _RULE_RE = re.compile(
        r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*"
        r"(IS\s+NOT\s+NULL|IS\s+NULL|>=|<=|==|!=|=|>|<)\s*"
        r"([-+]?[0-9]*\.?[0-9]+)?\s*$",
        re.IGNORECASE,
    )
    m = _RULE_RE.match(rule or "")
    if m:
        target_field, op, value = m.groups()
        op_norm = re.sub(r"\s+", "_", op.upper().strip())
        val = float(value) if value is not None else None
        return ConditionDef(operator=op_norm, left_field=target_field, value=val)
    return ConditionDef(operator="RAW_SQL_SHIM", value=rule)

def normalize_legacy_dict(raw: dict) -> dict:
    """Detect if the dict uses legacy keys (e.g. source_configuration, pipeline_stages)
    and normalize it into the canonical IR dict for Pydantic."""
    if "metadata" in raw and "sources" in raw and "stages" in raw:
        return raw # Already canonical

    # 1. Metadata
    old_meta = raw.get("pipeline_metadata", {})
    pipeline_name = old_meta.get("pipeline_name", "dpba_pipeline")
    meta = {
        "pipeline_name": pipeline_name,
        "version": old_meta.get("version", "1.0.0"),
        "description": old_meta.get("description", "")
    }

    # 2. Source
    old_src = raw.get("source_configuration", {})
    source_id = "primary_source"
    sources = [{
        "source_id": source_id,
        "type": "file",
        "format": old_src.get("format", "csv"),
        "location_ref": old_src.get("location", f"/mnt/raw/{pipeline_name}/"),
        "read_options": old_src.get("read_options") or {"header": "true", "inferSchema": "false"}
    }]

    # 3. Stages
    stages = []
    
    # helper for mapping
    def map_legacy_fields(legacy_mappings: list) -> list:
        mappings = []
        for m in legacy_mappings:
            src_fields = m.get("source_fields")
            if src_fields is None: src_fields = m.get("source_field")
            if not isinstance(src_fields, list): src_fields = [src_fields] if src_fields else []
            
            t = m.get("transformation", {})
            t_type = t.get("type", "direct")
            t_params = t.get("params", {})
            
            mappings.append({
                "target_field": m.get("target_field", ""),
                "target_type": m.get("target_data_type", "string"),
                "source_fields": src_fields,
                "is_nullable": m.get("is_nullable", True),
                "default_value": m.get("default_value"),
                "transformation": {"type": t_type, "params": t_params},
                "provenance": {"confidence": "high"}
            })
        return mappings

    def map_legacy_expectations(legacy_exps: list) -> list:
        exps = []
        for idx, e in enumerate(legacy_exps):
            rule_str = e.get("rule", "")
            cond = _parse_legacy_rule(rule_str)
            exps.append({
                "rule_id": e.get("expectation_name", f"rule_{idx}"),
                "name": e.get("expectation_name", f"rule_{idx}"),
                "target_fields": [cond.left_field] if cond.left_field else [],
                "condition": cond.model_dump() if hasattr(cond, "model_dump") else {"operator": cond.operator, "left_field": cond.left_field, "value": cond.value},
                "action": e.get("action_on_failure", "quarantine")
            })
        return exps

    if "pipeline_stages" in raw:
        prev_stage_id = source_id
        default_cat = "main"
        for i, st in enumerate(raw["pipeline_stages"]):
            s_name = st.get("stage_name", f"stage_{i}")
            tgt = st.get("target", {})
            cat, sch, tab = _split_table_name(tgt.get("table_name", ""), default_cat, s_name)
            default_cat = cat
            write_mode = tgt.get("write_mode")
            merge_keys = tgt.get("merge_keys", [])
            if not write_mode: write_mode = "merge" if merge_keys else "append"
            
            operations = {}
            if st.get("stage_type") == "transform":
                operations["mappings"] = map_legacy_fields(st.get("mappings", []))
                operations["expectations"] = map_legacy_expectations(st.get("data_expectations", []))
            
            stages.append({
                "stage_id": s_name,
                "inputs": [prev_stage_id],
                "operations": operations,
                "output": {
                    "table_name": f"{cat}.{sch}.{tab}",
                    "write_mode": write_mode,
                    "merge_keys": merge_keys
                }
            })
            prev_stage_id = s_name
    elif "target_configurations" in raw:
        # shorthand bronze/silver
        tgt = raw["target_configurations"]
        bronze = tgt.get("bronze_layer", {})
        silver = tgt.get("silver_layer", {})
        
        b_cat, b_sch, b_tab = _split_table_name(bronze.get("table_name", ""), "main", "bronze")
        stages.append({
            "stage_id": "bronze",
            "inputs": [source_id],
            "operations": {},
            "output": {
                "table_name": f"{b_cat}.{b_sch}.{b_tab}",
                "write_mode": bronze.get("write_mode", "append")
            }
        })
        
        s_cat, s_sch, s_tab = _split_table_name(silver.get("table_name", ""), b_cat, "silver")
        s_merge = silver.get("merge_keys", [])
        s_write = silver.get("write_mode", "merge" if s_merge else "append")
        stages.append({
            "stage_id": "silver",
            "inputs": ["bronze"],
            "operations": {
                "mappings": map_legacy_fields(raw.get("mappings", [])),
                "expectations": map_legacy_expectations(raw.get("data_expectations", []))
            },
            "output": {
                "table_name": f"{s_cat}.{s_sch}.{s_tab}",
                "write_mode": s_write,
                "merge_keys": s_merge
            }
        })

    # 4. Environments
    envs = []
    bundle_meta = raw.get("bundle_metadata", {})
    wf = raw.get("workflow_specification", {})
    cluster_cfg = wf.get("job_clusters", [{}])[0].get("new_cluster", {}) if wf.get("job_clusters") else {}
    
    envs.append({
        "name": "dev",
        "cluster": {
            "node_type": cluster_cfg.get("node_type_id", "m5d.large"),
            "workers": int(cluster_cfg.get("num_workers", 2)),
            "spark_version": cluster_cfg.get("spark_version", "15.4.x-scala2.12"),
            "spark_conf": cluster_cfg.get("spark_conf", {"spark.databricks.delta.preview.enabled": "true"})
        },
        "parameters": wf.get("parameters", {})
    })

    # 5. Tests
    old_test = raw.get("test_data_specifications", {})
    uc = old_test.get("unity_catalog", {})
    tests = {
        "synthetic_catalog": uc.get("catalog", "main"),
        "synthetic_schema": uc.get("schema", f"{stages[-1]['stage_id']}_test" if stages else "test"),
        "sample_inputs": old_test.get("sample_inputs", []),
        "sample_expected_outputs": old_test.get("expected_outputs", [])
    }

    return {
        "metadata": meta,
        "sources": sources,
        "stages": stages,
        "environments": envs,
        "tests": tests
    }

def check_capabilities(spec: PipelineSpec) -> None:
    """Validates that the provided valid IR uses features supported by the current generators."""
    from dpba.exceptions import UnsupportedOperationError
    
    for stage in spec.stages:
        if stage.operations.joins:
            raise UnsupportedOperationError("Joins are represented in the IR but not yet supported by Component 3.")
        
        for fm in stage.operations.mappings:
            t = fm.transformation.type.lower()
            if t not in SUPPORTED_TRANSFORMATION_TYPES:
                raise UnsupportedOperationError(f"Transformation '{t}' is represented in the IR but not supported by Component 3 (Supported: {SUPPORTED_TRANSFORMATION_TYPES}).")

def parse_mapping_dict(raw: dict, verbose: bool = True) -> PipelineSpec:
    canonical_dict = normalize_legacy_dict(raw)
    spec = PipelineSpec.model_validate(canonical_dict)
    check_capabilities(spec)
    return spec

def parse_mapping_json(json_path: str, verbose: bool = True) -> PipelineSpec:
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"Mapping JSON not found: {json_path}")
    with open(json_path, "r") as f:
        raw = json.load(f)
    try:
        return parse_mapping_dict(raw, verbose)
    except ValueError as e:
        raise ValueError(f"{json_path}: {e}")

def print_spec_table(spec: PipelineSpec) -> None:
    print(f"  Pipeline chain: {' → '.join(s.stage_id for s in spec.stages)}")
    for stage in spec.stages:
        if not stage.operations.mappings:
            continue
        print(f"\n  ── Stage '{stage.stage_id}'  (writes {stage.output.full_table}, merge_keys={stage.output.merge_keys or 'none'}) ──")
        hdr = f"  {'Target Field':<22} {'Source Field(s)':<24} {'Type':<14} {'Null':<6} {'Transform':<14}"
        print(hdr)
        print("  " + "─" * 110)
        for fm in stage.operations.mappings:
            src_display = " + ".join(fm.source_fields) if fm.source_fields else "(none)"
            print(
                f"  {fm.target_field:<22} {src_display:<24} {fm.target_type:<14} "
                f"{str(fm.is_nullable):<6} {fm.transformation.type:<14}"
            )
        if stage.operations.expectations:
            print()
            print("  Data Expectations:")
            for exp in stage.operations.expectations:
                print(f"    [{exp.action:<13}] {exp.name}: {exp.condition.operator}")
