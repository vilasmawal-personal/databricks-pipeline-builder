"""Requirement document to structured mapping. The model never generates code."""
import json
import logging
from pathlib import Path
from pydantic import ValidationError

from dpba.config import Settings
from dpba.exceptions import (
    AmbiguousMappingError, LLMOutputFormatError, LLMValidationError,
    MappingValidationError, UnsupportedOperationError,
)
from dpba.mapping_parser import parse_mapping_dict, PipelineSpec


logger = logging.getLogger(__name__)

# We use a static schema dict matching what the LLM expects (external format)
# The parser bridges this external JSON shape to the internal Pydantic PipelineSpec.
_TRANSFORMATION_SCHEMA = {
    "type": "object",
    "required": ["type", "params"],
    "properties": {
        "type": {"type": "string", "enum": ["direct", "cast", "concat", "conditional", "date_format", "conditional_date", "lookup", "split"]},
        "params": {"type": "object"},
        "logic": {"type": "string"},
    },
    "additionalProperties": True,
}

_MAPPING_SCHEMA = {
    "type": "object",
    "required": ["target_field", "source_fields", "target_data_type", "transformation"],
    "properties": {
        "target_field": {"type": "string", "minLength": 1},
        "source_fields": {"type": "array", "items": {"type": "string"}},
        "target_data_type": {"type": "string", "minLength": 1},
        "is_nullable": {"type": "boolean"},
        "default_value": {},
        "transformation": _TRANSFORMATION_SCHEMA,
        "provenance": {"type": "object"},
    },
    "additionalProperties": True,
}

SPEC_SCHEMA = {
    "type": "object",
    "required": ["pipeline_metadata", "source_configuration", "pipeline_stages"],
    "properties": {
        "pipeline_metadata": {
            "type": "object", "required": ["pipeline_name"],
            "properties": {"pipeline_name": {"type": "string", "minLength": 1}},
            "additionalProperties": True,
        },
        "source_configuration": {
            "type": "object", "required": ["format", "location"],
            "properties": {"format": {"type": "string"}, "location": {"type": "string"},
                           "read_options": {"type": "object"}},
            "additionalProperties": True,
        },
        "pipeline_stages": {
            "type": "array", "minItems": 2,
            "items": {
                "type": "object", "required": ["stage_name", "stage_type", "target"],
                "properties": {
                    "stage_name": {"type": "string", "minLength": 1},
                    "stage_type": {"type": "string", "enum": ["ingest", "transform"]},
                    "target": {"type": "object", "required": ["table_name"],
                               "properties": {"table_name": {"type": "string", "minLength": 1}},
                               "additionalProperties": True},
                    "mappings": {"type": "array", "items": _MAPPING_SCHEMA},
                    "data_expectations": {"type": "array", "items": {"type": "object"}},
                },
                "additionalProperties": True,
            },
        },
        "test_data_specifications": {"type": "object"},
        "bundle_metadata": {"type": "object"},
        "workflow_specification": {"type": "object"},
        "issues": {"type": "array", "items": {"type": "object"}},
    },
    "additionalProperties": True,
}


class MappingAgent:
    def __init__(self, llm, settings: Settings = None):
        self.llm = llm
        self.settings = settings or Settings()
        
        prompts_dir = Path(__file__).resolve().parent / "prompts"
        self.system_prompt = (prompts_dir / "component1_system.txt").read_text(encoding="utf-8")
        
        repair_path = prompts_dir / "component1_repair.txt"
        self.repair_prompt = repair_path.read_text(encoding="utf-8") if repair_path.exists() else (
            "Your previous JSON response failed validation:\n{errors}\n\n"
            "Original response:\n{previous_response}\n\n"
            "Please return ONLY the corrected JSON object matching the schema."
        )

    def generate(self, document: str) -> dict:
        if not isinstance(document, str) or not document.strip():
            raise LLMValidationError("The requirements document is empty.")
        if len(document) > self.settings.llm_max_document_chars:
            raise LLMValidationError(
                "Requirements document is {} characters; configured limit is {}. "
                "Split the document into smaller, linked specifications.".format(
                    len(document), self.settings.llm_max_document_chars)
            )

        original_prompt = self.system_prompt + "\n\n<requirements>\n" + document + "\n</requirements>"
        max_attempts = max(1, self.settings.llm_max_retries + 1)
        repair_context = ""
        previous_response = "{}"

        for attempt in range(1, max_attempts + 1):
            logger.info("Component 1 extraction/validation attempt %s/%s", attempt, max_attempts)
            try:
                prompt = original_prompt if attempt == 1 else self.repair_prompt.format(
                    attempt=attempt,
                    max_attempts=max_attempts,
                    errors=repair_context,
                    previous_response=previous_response,
                    requirements=document,
                )
                result = self.llm.generate_structured(prompt, SPEC_SCHEMA)
                if not isinstance(result, dict):
                    raise MappingValidationError("The mapping must be a JSON object.")
                previous_response = json.dumps(result, indent=2, ensure_ascii=False)

                self._check_forbidden_code(result)
                parse_mapping_dict(result, verbose=False)
                logger.info("Component 1 mapping passed structural and capability validation on attempt %s", attempt)
                return result

            except (ValidationError, MappingValidationError, UnsupportedOperationError,
                    AmbiguousMappingError, ValueError, TypeError, KeyError,
                    LLMOutputFormatError) as exc:
                repair_context = "{}: {}".format(type(exc).__name__, exc)
                logger.warning("Component 1 mapping rejected on attempt %s/%s: %s",
                               attempt, max_attempts, repair_context)
                if attempt == max_attempts:
                    raise LLMValidationError(
                        "No valid, fully supported mapping after {} attempt(s). Last validation error: {}".format(
                            max_attempts, repair_context)
                    ) from exc

        raise LLMValidationError("Mapping generation ended without a validated result.")


    def _check_forbidden_code(self, spec: dict) -> None:
        """Ensure the LLM didn't try to sneak executable code into the mapping."""
        forbidden = {"generated_code", "python_code", "pyspark_code", "sql_code", "executable"}
        problems = []

        def find_forbidden(value, path="mapping"):
            if isinstance(value, dict):
                for key, item in value.items():
                    if str(key).lower() in forbidden:
                        problems.append(f"{path} contains forbidden executable field {key!r}")
                    find_forbidden(item, f"{path}.{key}")
            elif isinstance(value, list):
                for index, item in enumerate(value):
                    find_forbidden(item, f"{path}[{index}]")

        find_forbidden(spec)
        
        # Also check for unresolved issues
        for index, issue in enumerate(spec.get("issues", [])):
            if isinstance(issue, dict) and str(issue.get("severity", "")).lower() in ("error", "critical", "unsupported", "ambiguous"):
                problems.append(f"unresolved issue {index + 1}: {issue.get('message', 'no message supplied')}")
                
        if problems:
            raise MappingValidationError("Invalid structured mapping: " + "; ".join(problems))


def read_mapping_document(path: str) -> dict:
    data = Path(path).read_text(encoding="utf-8")
    if Path(path).suffix.lower() == ".json":
        parsed = json.loads(data)
        # Validate the manually supplied JSON immediately
        parse_mapping_dict(parsed, verbose=False)
        return parsed
    return data
