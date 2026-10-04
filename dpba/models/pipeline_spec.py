import re
from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field, model_validator

class PipelineMetadata(BaseModel):
    pipeline_name: str
    version: str = "1.0.0"
    description: str = ""

class SourceDefinition(BaseModel):
    source_id: str
    type: str # file, table, etc
    format: str
    location_ref: str
    read_options: Dict[str, str] = {}

class ProvenanceMetadata(BaseModel):
    confidence: Literal["high", "low", "human_required"] = "high"
    source_reference: str = ""
    assumptions: str = ""

class TransformationDef(BaseModel):
    type: str
    params: Dict[str, Any] = {}

class FieldMapping(BaseModel):
    target_field: str
    target_type: str
    source_fields: List[str] = []
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

class ConditionDef(BaseModel):
    operator: str
    left_field: Optional[str] = None
    right_field: Optional[str] = None
    value: Optional[Any] = None
    
    @property
    def is_sql_string_shim(self) -> bool:
        return self.operator == "RAW_SQL_SHIM"

class JoinDefinition(BaseModel):
    left_input: str
    right_input: str
    join_type: str = "left"
    conditions: List[ConditionDef] = []

class DataExpectation(BaseModel):
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

class StageOperations(BaseModel):
    joins: List[JoinDefinition] = []
    mappings: List[FieldMapping] = []
    expectations: List[DataExpectation] = []

class SchemaEvolutionStrategy(BaseModel):
    allow_new_columns: bool = False
    allow_type_changes: bool = False
    allow_column_removal: bool = False

class OutputDefinition(BaseModel):
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

class PipelineStage(BaseModel):
    stage_id: str
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

class ClusterDef(BaseModel):
    node_type: str = "m5d.large"
    workers: int = 2
    spark_version: str = "15.4.x-scala2.12"
    spark_conf: Dict[str, str] = {}

class EnvironmentSpec(BaseModel):
    name: str
    cluster: ClusterDef = Field(default_factory=ClusterDef)
    parameters: Dict[str, str] = {}

class TestSpecification(BaseModel):
    synthetic_catalog: str = "main"
    synthetic_schema: str = "test_fixtures"
    sample_inputs: List[Dict[str, Any]] = []
    sample_expected_outputs: List[Dict[str, Any]] = []

class PipelineSpec(BaseModel):
    metadata: PipelineMetadata
    sources: List[SourceDefinition]
    stages: List[PipelineStage]
    environments: List[EnvironmentSpec] = []
    tests: TestSpecification = Field(default_factory=TestSpecification)
    
    @model_validator(mode="after")
    def _check_ambiguity(self) -> "PipelineSpec":
        for stage in self.stages:
            for mapping in stage.operations.mappings:
                if mapping.provenance.confidence == "human_required" or mapping.transformation.type == "UNKNOWN":
                    from dpba.exceptions import AmbiguousMappingError
                    raise AmbiguousMappingError(
                        f"Field '{mapping.target_field}' in stage '{stage.stage_id}' requires human review: {mapping.provenance.assumptions}"
                    )
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
        return self.stages[-1].stage_id

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
        return f"{self.metadata.pipeline_name}_bundle"

    @property
    def job_name(self) -> str:
        return f"{self.metadata.pipeline_name}_workflow"

    @property
    def job_description(self) -> str:
        return self.metadata.description

    @property
    def target_environments(self) -> List[str]:
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
