import re
from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator


def _valid_spark_type(value: str) -> bool:
    normalized = value.lower().strip()
    supported = {"string", "boolean", "bool", "date", "timestamp", "integer", "int", "long", "double"}
    if normalized in supported:
        return True
    match = re.fullmatch(r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)", normalized)
    return bool(match and 1 <= int(match.group(1)) <= 38 and 0 <= int(match.group(2)) <= int(match.group(1)))


class SpecModel(BaseModel):
    """Reject misspelled or silently ignored fields at the IR boundary."""
    model_config = ConfigDict(extra="forbid")

class PipelineMetadata(SpecModel):
    pipeline_name: str
    version: str = "1.0.0"
    description: str = ""

class SourceDefinition(SpecModel):
    source_id: str
    type: Literal["file", "table"]
    format: str
    location_ref: str
    read_options: Dict[str, str] = {}

class ProvenanceMetadata(SpecModel):
    confidence: Literal["high", "low", "human_required"] = "high"
    source_reference: str = ""
    assumptions: str = ""

class TransformationDef(SpecModel):
    type: str
    params: Dict[str, Any] = {}

class FieldMapping(SpecModel):
    target_field: str = Field(min_length=1, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    target_type: str
    source_fields: List[str] = Field(default_factory=list)
    transformation: TransformationDef
    provenance: ProvenanceMetadata = Field(default_factory=ProvenanceMetadata)
    is_nullable: bool = True
    default_value: Any = None
    
    @property
    def primary_source(self) -> str:
        return self.source_fields[0] if self.source_fields else ""

    @property
    def transformation_type(self) -> str:
        return self.transformation.type
        
    @property
    def transformation_params(self) -> dict:
        return self.transformation.params

    @property
    def transformation_logic(self) -> str:
        return ""
        
    @property
    def spark_type_literal(self) -> str:
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
        m = re.match(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$", self.target_type)
        if m:
            return f"DecimalType({m.group(1)}, {m.group(2)})"
        return _SPARK_TYPE_MAP.get(self.target_type, "StringType()")
        
    @property
    def simple_type_string(self) -> str:
        m = re.match(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$", self.target_type)
        if m:
            return f"decimal({m.group(1)},{m.group(2)})"
        return {"integer": "int", "bool": "boolean"}.get(self.target_type, self.target_type)
        
    @property
    def target_data_type(self) -> str:
        return self.target_type

class ConditionDef(SpecModel):
    operator: str
    left_field: Optional[str] = None
    right_field: Optional[str] = None
    value: Optional[Any] = None
    
    @property
    def is_sql_string_shim(self) -> bool:
        return self.operator == "RAW_SQL_SHIM"

class JoinDefinition(SpecModel):
    left_input: str
    right_input: str
    join_type: str = "left"
    conditions: List[ConditionDef] = []

class DataExpectation(SpecModel):
    rule_id: str
    name: str
    target_fields: List[str] = []
    condition: ConditionDef
    action: Literal["drop_row", "quarantine", "fail_pipeline"] = "quarantine"

    @property
    def action_on_failure(self) -> str:
        return self.action

    @property
    def rule(self) -> str:
        if self.condition.is_sql_string_shim:
            return self.condition.value
        return f"{self.condition.left_field} {self.condition.operator} {self.condition.value or ''}".strip()

    @property
    def parsed(self) -> Optional[dict]:
        if self.condition.is_sql_string_shim:
            return None
        return {
            "field": self.condition.left_field,
            "op": self.condition.operator.replace("_", " "),
            "value": self.condition.value
        }

class StageOperations(SpecModel):
    joins: List[JoinDefinition] = []
    mappings: List[FieldMapping] = []
    expectations: List[DataExpectation] = []

class SchemaEvolutionStrategy(SpecModel):
    allow_new_columns: bool = False
    allow_type_changes: bool = False
    allow_column_removal: bool = False

class OutputDefinition(SpecModel):
    table_name: str
    write_mode: Literal["append", "overwrite", "merge"] = "append"
    merge_keys: List[str] = []
    schema_evolution: SchemaEvolutionStrategy = Field(default_factory=SchemaEvolutionStrategy)
    
    @property
    def full_table(self) -> str:
        parts = self.table_name.split(".")
        if len(parts) == 3: return self.table_name
        return f"main.default.{self.table_name}"
        
    @property
    def catalog(self) -> str:
        return self.full_table.split(".")[0]
        
    @property
    def schema(self) -> str:
        return self.full_table.split(".")[1]
        
    @property
    def table(self) -> str:
        return self.full_table.split(".")[2]

class PipelineStage(SpecModel):
    stage_id: str = Field(min_length=1, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    inputs: List[str] = []
    operations: StageOperations = Field(default_factory=StageOperations)
    output: OutputDefinition

    @property
    def name(self) -> str:
        return self.stage_id

    @property
    def full_table(self) -> str:
        return self.output.full_table

    @property
    def catalog(self) -> str:
        return self.output.catalog

    @property
    def schema(self) -> str:
        return self.output.schema

    @property
    def table(self) -> str:
        return self.output.table

    @property
    def format(self) -> str:
        return "delta"

    @property
    def write_mode(self) -> str:
        return self.output.write_mode

    @property
    def merge_keys(self) -> List[str]:
        return self.output.merge_keys

    @property
    def fields(self) -> List[FieldMapping]:
        return self.operations.mappings

    @property
    def expectations(self) -> List[DataExpectation]:
        return self.operations.expectations

    @property
    def all_target_fields(self) -> List[str]:
        return [fm.target_field for fm in self.fields]

    @property
    def required_fields(self) -> List[FieldMapping]:
        return [f for f in self.fields if not f.is_nullable]

    @property
    def all_source_columns(self) -> List[str]:
        cols = []
        for fm in self.fields:
            cols.extend(fm.source_fields)
        return list(dict.fromkeys(cols))

class ClusterDef(SpecModel):
    node_type: str = "m5d.large"
    workers: int = 2
    spark_version: str = "15.4.x-scala2.12"
    spark_conf: Dict[str, str] = {}

class EnvironmentSpec(SpecModel):
    name: str
    cluster: ClusterDef = Field(default_factory=ClusterDef)
    parameters: Dict[str, str] = {}

class TestSpecification(SpecModel):
    synthetic_catalog: str = "main"
    synthetic_schema: str = "test_fixtures"
    table_prefix: Optional[str] = None
    sample_inputs: List[Dict[str, Any]] = []
    sample_expected_outputs: List[Dict[str, Any]] = []


class DeploymentSpec(SpecModel):
    bundle_name: Optional[str] = None
    job_name: Optional[str] = None
    target_environments: List[str] = []

class PipelineSpec(SpecModel):
    metadata: PipelineMetadata
    sources: List[SourceDefinition] = Field(min_length=1, max_length=1)
    stages: List[PipelineStage] = Field(min_length=2)
    environments: List[EnvironmentSpec] = []
    tests: TestSpecification = Field(default_factory=TestSpecification)
    deployment: DeploymentSpec = Field(default_factory=DeploymentSpec)
    
    @model_validator(mode="after")
    def _check_ambiguity(self) -> "PipelineSpec":
        for stage_index, stage in enumerate(self.stages):
            if stage_index == 0:
                if stage.inputs != [self.sources[0].source_id]:
                    raise ValueError("The ingest stage must read exactly the declared primary source.")
                if stage.fields or stage.expectations or stage.operations.joins:
                    raise ValueError("The ingest stage is a raw copy and cannot declare transforms or expectations.")
            elif stage.inputs != [self.stages[stage_index - 1].stage_id]:
                raise ValueError(
                    "Only a linear single-input pipeline is supported: stage {!r} must read {!r}.".format(
                        stage.stage_id, self.stages[stage_index - 1].stage_id)
                )
            if stage_index > 0 and not stage.fields:
                raise ValueError("Transform stage {!r} must declare at least one field mapping.".format(stage.stage_id))

            for mapping in stage.operations.mappings:
                if mapping.provenance.confidence != "high" or mapping.transformation.type == "UNKNOWN":
                    from dpba.exceptions import AmbiguousMappingError
                    raise AmbiguousMappingError(
                        "Field {!r} in stage {!r} is not cleared for automatic generation (confidence={!r}): {}".format(
                            mapping.target_field, stage.stage_id, mapping.provenance.confidence,
                            mapping.provenance.assumptions or "human review is required")
                    )

            targets = [mapping.target_field for mapping in stage.fields]
            if len(targets) != len(set(targets)):
                raise ValueError("Stage {!r} contains duplicate target fields.".format(stage.stage_id))

            if stage_index > 1:
                available = set(self.stages[stage_index - 1].all_target_fields)
                for mapping in stage.fields:
                    missing = set(mapping.source_fields) - available
                    if missing:
                        raise ValueError("Stage {!r}, field {!r} references unavailable prior-stage fields: {}".format(
                            stage.stage_id, mapping.target_field, sorted(missing)))

            for key in stage.merge_keys:
                if key not in targets:
                    raise ValueError("Merge key {!r} is not mapped in stage {!r}.".format(key, stage.stage_id))
            if stage.write_mode == "merge" and not stage.merge_keys:
                raise ValueError("Stage {!r} declares merge mode without merge_keys.".format(stage.stage_id))
            if stage.write_mode != "merge" and stage.merge_keys:
                raise ValueError("Stage {!r} declares merge_keys but write_mode is {!r}.".format(stage.stage_id, stage.write_mode))

            for expectation in stage.expectations:
                condition = expectation.condition
                if condition.operator not in {"IS_NOT_NULL", "IS_NULL", ">=", "<=", ">", "<", "==", "=", "!="}:
                    raise ValueError("Expectation {!r} uses unsupported operator {!r}.".format(
                        expectation.name, condition.operator))
                if not condition.left_field or condition.left_field not in targets:
                    raise ValueError("Expectation {!r} references a field not mapped in stage {!r}.".format(
                        expectation.name, stage.stage_id))
                if condition.operator not in {"IS_NOT_NULL", "IS_NULL"} and not isinstance(condition.value, (int, float)):
                    raise ValueError("Expectation {!r} requires a numeric comparison value.".format(expectation.name))

        stage_ids = [stage.stage_id for stage in self.stages]
        if len(stage_ids) != len(set(stage_ids)):
            raise ValueError("Stage identifiers must be unique.")
        source_ids = [source.source_id for source in self.sources]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("Source identifiers must be unique.")
        for mapping in (mapping for stage in self.stages for mapping in stage.fields):
            bad_sources = [name for name in mapping.source_fields
                           if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name)]
            if bad_sources:
                raise ValueError("Field {!r} contains unsupported source identifiers: {}".format(
                    mapping.target_field, bad_sources))
            if not _valid_spark_type(mapping.target_type):
                raise ValueError("Unsupported target type {!r} for field {!r}.".format(
                    mapping.target_type, mapping.target_field))
            params = mapping.transformation.params
            kind = mapping.transformation.type.lower()
            if kind == "conditional":
                divisor = params.get("divisor", 1)
                if not isinstance(divisor, (int, float)) or isinstance(divisor, bool) or divisor == 0:
                    raise ValueError("conditional transformation for {!r} needs a non-zero numeric divisor.".format(mapping.target_field))
                if mapping.target_type.lower() not in {"int", "integer", "long", "double"} and not mapping.target_type.lower().startswith("decimal("):
                    raise ValueError("conditional transformation for {!r} needs a numeric target type.".format(mapping.target_field))
            elif kind == "concat":
                if len(mapping.source_fields) < 2 or not isinstance(params.get("separator", " "), str):
                    raise ValueError("concat transformation for {!r} needs at least two sources and a string separator.".format(mapping.target_field))
            elif kind in {"date_format", "conditional_date"}:
                formats = params.get("source_formats")
                if formats is not None and (not isinstance(formats, list) or not formats or not all(isinstance(fmt, str) and fmt for fmt in formats)):
                    raise ValueError("source_formats for {!r} must be a non-empty array of strings.".format(mapping.target_field))
                out_fmt = params.get("output_format")
                if out_fmt is not None and (not isinstance(out_fmt, str) or not out_fmt or mapping.target_type.lower() != "string"):
                    raise ValueError("output_format for {!r} requires a string target type.".format(mapping.target_field))
                sentinel = params.get("sentinel_value")
                if sentinel is not None and not isinstance(sentinel, str):
                    raise ValueError("sentinel_value for {!r} must be a string.".format(mapping.target_field))
            elif kind == "lookup":
                value_map = params.get("value_map")
                if not isinstance(value_map, dict) or not value_map or not all(isinstance(key, str) for key in value_map):
                    raise ValueError("lookup transformation for {!r} needs a non-empty string-keyed value_map.".format(mapping.target_field))
                if not isinstance(params.get("case_insensitive", True), bool):
                    raise ValueError("case_insensitive for {!r} must be boolean.".format(mapping.target_field))
            elif kind == "split":
                delimiter, index = params.get("delimiter", " "), params.get("index", 0)
                if not isinstance(delimiter, str) or not delimiter or not isinstance(index, int) or isinstance(index, bool) or index < 0:
                    raise ValueError("split for {!r} needs a non-empty delimiter and non-negative integer index.".format(mapping.target_field))
        return self

    @property
    def ingest_stage(self) -> PipelineStage:
        return self.stages[0]

    @property
    def transform_stages(self) -> List[PipelineStage]:
        return self.stages[1:] if len(self.stages) > 1 else []

    @property
    def final_stage(self) -> PipelineStage:
        return self.stages[-1]

    @property
    def raw_source_columns(self) -> List[str]:
        if len(self.stages) > 1:
            return self.stages[1].all_source_columns
        return []

    @property
    def source_read_options(self) -> Dict[str, str]:
        return self.sources[0].read_options if self.sources else {}

    @property
    def source_location(self) -> str:
        return self.sources[0].location_ref if self.sources else ""

    @property
    def source_system(self) -> str:
        return self.sources[0].source_id if self.sources else "unknown"

    @property
    def source_format(self) -> str:
        return self.sources[0].format if self.sources else "csv"

    @property
    def target_catalog(self) -> str:
        return self.stages[0].catalog if self.stages else "main"

    @property
    def test_catalog(self) -> str:
        return self.tests.synthetic_catalog

    @property
    def test_schema(self) -> str:
        return self.tests.synthetic_schema

    @property
    def test_table_prefix(self) -> str:
        return self.tests.table_prefix or self.stages[-1].stage_id

    @property
    def test_synthetic_input_table(self) -> str:
        return f"{self.tests.synthetic_catalog}.{self.tests.synthetic_schema}.{self.stages[-1].stage_id}_synthetic_input"

    @property
    def test_expected_output_table(self) -> str:
        return f"{self.tests.synthetic_catalog}.{self.tests.synthetic_schema}.{self.stages[-1].stage_id}_expected_output"

    @property
    def sample_inputs(self) -> List[dict]:
        return self.tests.sample_inputs

    @property
    def sample_expected_outputs(self) -> List[dict]:
        return self.tests.sample_expected_outputs

    @property
    def pipeline_name(self) -> str:
        return self.metadata.pipeline_name

    @property
    def primary_key_field(self) -> FieldMapping:
        final = self.final_stage
        if final.merge_keys:
            for fm in final.fields:
                if fm.target_field == final.merge_keys[0]: return fm
        for fm in final.fields:
            if not fm.is_nullable: return fm
        return final.fields[0] if final.fields else None

    @property
    def bundle_name(self) -> str:
        return self.deployment.bundle_name or f"{self.metadata.pipeline_name}_bundle"

    @property
    def job_name(self) -> str:
        return self.deployment.job_name or f"{self.metadata.pipeline_name}_workflow"

    @property
    def job_description(self) -> str:
        return self.metadata.description

    @property
    def target_environments(self) -> List[str]:
        if self.deployment.target_environments:
            return self.deployment.target_environments
        return [e.name for e in self.environments] if self.environments else ["dev"]
        
    @property
    def cluster_spark_version(self) -> str:
        return self.environments[0].cluster.spark_version if self.environments else "15.4.x-scala2.12"
        
    @property
    def cluster_node_type_id(self) -> str:
        return self.environments[0].cluster.node_type if self.environments else "m5d.large"
        
    @property
    def cluster_num_workers(self) -> int:
        return self.environments[0].cluster.workers if self.environments else 2
        
    @property
    def cluster_spark_conf(self) -> Dict[str, str]:
        return self.environments[0].cluster.spark_conf if self.environments else {}

    @property
    def job_parameters(self) -> Dict[str, str]:
        return self.environments[0].parameters if self.environments else {}
