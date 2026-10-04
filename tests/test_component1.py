import json
from pathlib import Path
from urllib.error import URLError

import pytest

from dpba.component1 import MappingAgent, validate_mapping_shape, read_mapping_document
from dpba.config import Settings
from dpba.exceptions import LLMResponseError, MappingValidationError
from dpba.llm import FakeLLMClient, OllamaLLMClient


SPEC = {
    "pipeline_metadata": {"pipeline_name": "x"},
    "source_configuration": {"format": "csv"},
    "pipeline_stages": [
        {"stage_name": "raw", "stage_type": "ingest"},
        {"stage_name": "clean", "stage_type": "transform", "mappings": []},
    ],
}


def test_mapping_agent_returns_structured_spec_and_uses_prompt():
    llm = FakeLLMClient(SPEC)
    result = MappingAgent(llm).generate("Map source id to target id")
    assert result == SPEC


def test_mapping_agent_rejects_invalid_model_shape():
    with pytest.raises(MappingValidationError, match="pipeline_stages"):
        MappingAgent(FakeLLMClient({"pipeline_metadata": {}})).generate("requirements")


def test_mapping_agent_rejects_executable_output():
    response = dict(SPEC, generated_code="print('unsafe')")
    with pytest.raises(MappingValidationError, match="forbidden executable"):
        MappingAgent(FakeLLMClient(response)).generate("requirements")


def test_shape_validator_accepts_legacy_mapping():
    legacy = {"target_configurations": {"bronze_layer": {}, "silver_layer": {}}, "mappings": []}
    assert validate_mapping_shape(legacy) is legacy


def test_json_document_is_not_sent_to_llm(tmp_path):
    path = tmp_path / "mapping.json"
    path.write_text(json.dumps(SPEC))
    assert read_mapping_document(str(path)) == SPEC


def test_settings_are_configurable(monkeypatch):
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://ollama.internal:11434/")
    monkeypatch.setenv("OLLAMA_MODEL", "qwen3.6-custom")
    cfg = Settings.from_env()
    assert cfg.ollama_base_url == "http://ollama.internal:11434"
    assert cfg.ollama_model == "qwen3.6-custom"


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

    mapping = json.loads((Path(__file__).parents[1] / "sample_specs" / "customer_pipeline.json").read_text())
    generated = MappingAgent(FakeLLMClient(mapping)).generate("Customer mapping")
    spec_path = tmp_path / "spec.json"
    spec_path.write_text(json.dumps(generated))
    spec = parse_mapping_json(str(spec_path), verbose=False)
    cases = run_component2(spec, str(tmp_path / "fixtures"), skip_hitl=True)
    artifacts = run_component3(spec, str(tmp_path / "bundle"), cases, skip_hitl=True)
    assert cases
    assert "job_yml" in artifacts
    assert (tmp_path / "bundle" / "notebooks" / "01_ingest_bronze.py").exists()
