import json
from pathlib import Path
from urllib.error import URLError

import pytest
from pydantic import ValidationError

from dpba.component1 import MappingAgent, read_mapping_document
from dpba.config import Settings
from dpba.exceptions import LLMResponseError, LLMValidationError, MappingValidationError
from dpba.llm import FakeLLMClient, OllamaLLMClient
from dpba.models.pipeline_spec import PipelineSpec


SPEC = {
    "metadata": {"pipeline_name": "x", "version": "1.0.0", "description": "desc"},
    "sources": [
        {"source_id": "src", "type": "file", "format": "csv", "location_ref": "/", "read_options": {}}
    ],
    "stages": [
        {
            "stage_id": "raw", "inputs": ["src"], "operations": {"mappings": [], "expectations": []},
            "output": {"table_name": "main.raw.t", "write_mode": "append"}
        },
        {
            "stage_id": "clean", "inputs": ["raw"], 
            "operations": {
                "mappings": [
                    {
                        "target_field": "id",
                        "source_fields": ["id_raw"],
                        "target_type": "string",
                        "is_nullable": True,
                        "transformation": {"type": "direct", "params": {}},
                        "provenance": {"confidence": "high"}
                    }
                ],
                "expectations": []
            },
            "output": {"table_name": "main.clean.t", "write_mode": "append"}
        }
    ],
    "environments": [],
    "tests": {"synthetic_catalog": "main", "synthetic_schema": "test_schema", "sample_inputs": [], "sample_expected_outputs": []}
}


def test_mapping_agent_returns_structured_spec_and_uses_prompt():
    llm = FakeLLMClient(SPEC)
    agent = MappingAgent(llm, Settings(llm_max_retries=0))
    result = agent.generate("Map source id to target id")
    assert result == SPEC


def test_mapping_agent_rejects_invalid_model_shape_and_retries():
    # We will pass invalid spec. FakeLLMClient will return it twice.
    # The agent should retry and finally raise LLMValidationError.
    invalid_spec = {"metadata": {}, "sources": [], "stages": [], "environments": [], "tests": {}}
    llm = FakeLLMClient(invalid_spec)
    agent = MappingAgent(llm, Settings(llm_max_retries=1))
    
    with pytest.raises(LLMValidationError, match="Final validation error:"):
        agent.generate("requirements")


def test_mapping_agent_rejects_executable_output():
    response = dict(SPEC, generated_code="print('unsafe')")
    llm = FakeLLMClient(response)
    agent = MappingAgent(llm, Settings(llm_max_retries=0))
    with pytest.raises(LLMValidationError, match="forbidden executable"):
        agent.generate("requirements")


def test_json_document_is_not_sent_to_llm(tmp_path):
    path = tmp_path / "mapping.json"
    path.write_text(json.dumps(SPEC))
    assert read_mapping_document(str(path)) == SPEC


def test_settings_are_configurable(monkeypatch):
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://ollama.internal:11434")
    monkeypatch.setenv("OLLAMA_MODEL", "qwen3.6-custom")
    monkeypatch.setenv("DPBA_LLM_MAX_RETRIES", "5")
    cfg = Settings.from_env()
    assert cfg.ollama_base_url == "http://ollama.internal:11434"
    assert cfg.ollama_model == "qwen3.6-custom"
    assert cfg.llm_max_retries == 5


def test_ollama_structured_uses_json_schema_and_parses(monkeypatch):
    seen = {}

    class Response:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self): return json.dumps({"response": json.dumps(SPEC)}).encode()

    def fake_urlopen(request, timeout):
        seen["url"] = request.full_url
        seen["payload"] = json.loads(request.data.decode())
        return Response()

    monkeypatch.setattr("dpba.llm.urlopen", fake_urlopen)
    schema = {"type": "object"}
    result = OllamaLLMClient().generate_structured("prompt", schema)
    assert result == SPEC
    assert seen["url"] == "http://localhost:11434/api/generate"
    assert seen["payload"]["format"] == schema
    assert seen["payload"]["options"]["temperature"] == 0


def test_ollama_unavailable_has_actionable_error(monkeypatch):
    monkeypatch.setattr("dpba.llm.urlopen", lambda *a, **k: (_ for _ in ()).throw(URLError("offline")))
    with pytest.raises(LLMResponseError, match="OLLAMA_BASE_URL"):
        OllamaLLMClient().generate("prompt")


def test_fake_component1_output_hands_off_to_components2_and3(tmp_path):
    from component2_data_generator import run as run_component2
    from component3_pipeline_builder import run as run_component3
    from mapping_parser import parse_mapping_json

    mapping = json.loads((Path(__file__).parent / "fixtures" / "sample_specs" / "customer_pipeline.json").read_text())
    
    agent = MappingAgent(FakeLLMClient(mapping), Settings(llm_max_retries=0))
    generated = agent.generate("Customer mapping")
    
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps(generated))
    spec = parse_mapping_json(str(spec_path), verbose=False)
    
    cases = run_component2(spec, str(tmp_path / "fixtures"), skip_hitl=True)
    artifacts = run_component3(spec, str(tmp_path / "bundle"), cases, skip_hitl=True)
    
    assert cases
    assert "job_yml" in artifacts
    assert (tmp_path / "bundle" / "notebooks" / "01_ingest_bronze.py").exists()
