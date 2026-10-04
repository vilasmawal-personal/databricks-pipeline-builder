"""Requirement document to structured mapping. The model never generates code."""
import json
from pathlib import Path

from dpba.exceptions import MappingValidationError

SPEC_SCHEMA = {
    "type": "object",
    "required": ["pipeline_metadata", "source_configuration", "pipeline_stages"],
    "properties": {
        "pipeline_metadata": {"type": "object"},
        "source_configuration": {"type": "object"},
        "pipeline_stages": {"type": "array", "minItems": 2},
        "test_data_specifications": {"type": "object"},
        "bundle_metadata": {"type": "object"},
        "workflow_specification": {"type": "object"},
        "issues": {"type": "array"},
    },
    "additionalProperties": True,
}


class MappingAgent:
    def __init__(self, llm, system_prompt=None):
        self.llm = llm
        path = Path(__file__).resolve().parent.parent / "prompts" / "component1_system.txt"
        self.system_prompt = system_prompt if system_prompt is not None else path.read_text(encoding="utf-8")

    def generate(self, document):
        prompt = self.system_prompt + "\n\n<requirements>\n" + document + "\n</requirements>"
        result = self.llm.generate_structured(prompt, SPEC_SCHEMA)
        validate_mapping_shape(result)
        return result


def validate_mapping_shape(spec):
    problems = []
    if not isinstance(spec, dict):
        raise MappingValidationError("Pipeline mapping must be a JSON object.")
    forbidden = {"generated_code", "python_code", "pyspark_code", "sql_code", "executable"}

    def find_forbidden(value, path="mapping"):
        if isinstance(value, dict):
            for key, item in value.items():
                if str(key).lower() in forbidden:
                    problems.append("{} contains forbidden executable field {!r}".format(path, key))
                find_forbidden(item, "{}.{}".format(path, key))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                find_forbidden(item, "{}[{}]".format(path, index))

    find_forbidden(spec)
    legacy = isinstance(spec.get("target_configurations"), dict)
    for key in ("pipeline_metadata", "source_configuration"):
        if key in spec and not isinstance(spec.get(key), dict):
            problems.append("{} must be an object".format(key))
    if not legacy:
        for key in ("pipeline_metadata", "source_configuration"):
            if not isinstance(spec.get(key), dict):
                problems.append("{} must be an object".format(key))
    stages = spec.get("pipeline_stages")
    if legacy:
        if not isinstance(spec.get("target_configurations", {}).get("bronze_layer"), dict) or not isinstance(
                spec.get("target_configurations", {}).get("silver_layer"), dict):
            problems.append("legacy target_configurations must contain bronze_layer and silver_layer objects")
    elif not isinstance(stages, list) or len(stages) < 2:
        problems.append("pipeline_stages must contain an ingest stage and at least one transform stage")
    elif stages:
        if not all(isinstance(stage, dict) for stage in stages):
            problems.append("each pipeline_stages entry must be an object")
        elif stages[0].get("stage_type", "ingest") != "ingest":
            problems.append("the first pipeline stage must have stage_type 'ingest'")
        else:
            for i, stage in enumerate(stages[1:], 1):
                if stage.get("stage_type", "transform") != "transform":
                    problems.append("pipeline_stages[{}] must have stage_type 'transform'".format(i))
    for index, issue in enumerate(spec.get("issues", [])):
        if isinstance(issue, dict) and str(issue.get("severity", "")).lower() in ("error", "critical", "unsupported", "ambiguous"):
            problems.append("unresolved issue {}: {}".format(index + 1, issue.get("message", "no message supplied")))
    if problems:
        raise MappingValidationError("Invalid structured mapping: " + "; ".join(problems))
    return spec


def read_mapping_document(path):
    data = Path(path).read_text(encoding="utf-8")
    if Path(path).suffix.lower() == ".json":
        parsed = json.loads(data)
        validate_mapping_shape(parsed)
        return parsed
    return data
