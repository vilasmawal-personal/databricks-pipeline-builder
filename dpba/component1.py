"""Requirement document to structured mapping. The model never generates code."""
import json
import logging
from pathlib import Path
from pydantic import ValidationError

from dpba.config import Settings
from dpba.exceptions import LLMValidationError, MappingValidationError
from mapping_parser import parse_mapping_dict, PipelineSpec


logger = logging.getLogger(__name__)

# We use a static schema dict matching what the LLM expects (external format)
# The parser bridges this external JSON shape to the internal Pydantic PipelineSpec.
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
    def __init__(self, llm, settings: Settings = None):
        self.llm = llm
        self.settings = settings or Settings()
        
        prompts_dir = Path(__file__).resolve().parent.parent / "prompts"
        self.system_prompt = (prompts_dir / "component1_system.txt").read_text(encoding="utf-8")
        
        repair_path = prompts_dir / "component1_repair.txt"
        self.repair_prompt = repair_path.read_text(encoding="utf-8") if repair_path.exists() else (
            "Your previous JSON response failed validation:\n{errors}\n\n"
            "Original response:\n{previous_response}\n\n"
            "Please return ONLY the corrected JSON object matching the schema."
        )

    def generate(self, document: str) -> dict:
        prompt = self.system_prompt + "\n\n<requirements>\n" + document + "\n</requirements>"
        
        # Initial generation
        logger.info("Sending requirements to LLM for structured extraction...")
        result = self.llm.generate_structured(prompt, SPEC_SCHEMA)
        
        # Retry loop for validation failures
        max_retries = self.settings.llm_max_retries
        for attempt in range(max_retries + 1):
            try:
                # 1. Check for forbidden executable code (business rule, not just schema)
                self._check_forbidden_code(result)
                
                # 2. Parse and validate the structure
                parse_mapping_dict(result, verbose=False)
                
                # If we get here, it's valid
                if attempt > 0:
                    logger.info(f"✅ LLM successfully repaired the JSON on attempt {attempt}")
                return result
                
            except (ValidationError, MappingValidationError, ValueError) as e:
                if attempt == max_retries:
                    logger.error(f"❌ LLM failed to produce valid JSON after {max_retries} retries.")
                    raise LLMValidationError(f"Final validation error: {str(e)}") from e
                
                logger.warning(f"⚠️ LLM produced invalid JSON (attempt {attempt + 1}). Requesting repair...")
                
                # Format the repair prompt
                repair_msg = self.repair_prompt.format(
                    errors=str(e),
                    previous_response=json.dumps(result, indent=2)
                )
                
                # We ask the LLM to fix it
                result = self.llm.generate_structured(repair_msg, SPEC_SCHEMA)
                
        return result


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
