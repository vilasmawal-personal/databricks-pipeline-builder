"""Small local-only LLM abstraction and Ollama structured-output client."""
import json
from typing import Protocol
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

from dpba.exceptions import LLMOutputFormatError, LLMResponseError


class LLMClient(Protocol):
    def generate(self, prompt: str) -> str: ...
    def generate_structured(self, prompt: str, schema: dict) -> dict: ...


class OllamaLLMClient:
    def __init__(self, base_url="http://localhost:11434", model="qwen3.6", timeout=180,
                 max_output_tokens=4096, num_ctx=8192):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self.max_output_tokens = max_output_tokens
        self.num_ctx = num_ctx

    def _request(self, payload):
        request = Request(self.base_url + "/api/generate", data=json.dumps(payload).encode("utf-8"),
                          headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except (URLError, HTTPError, TimeoutError, ValueError) as exc:
            raise LLMResponseError(
                "Could not get a response from local Ollama at {} using model {!r}: {}. "
                "Check OLLAMA_BASE_URL, OLLAMA_MODEL, and that the model is available.".format(
                    self.base_url, self.model, exc)) from exc

    def generate(self, prompt):
        result = self._request({"model": self.model, "prompt": prompt, "stream": False})
        text = result.get("response")
        if not isinstance(text, str):
            raise LLMResponseError("Ollama response did not contain text.")
        return text

    def generate_structured(self, prompt, schema):
        result = self._request({"model": self.model, "prompt": prompt, "stream": False, "think": False,
                                "format": schema, "options": {
                                    "temperature": 0,
                                    "num_predict": self.max_output_tokens,
                                    "num_ctx": self.num_ctx,
                                }})
        try:
            value = json.loads(result["response"])
        except (KeyError, TypeError, ValueError) as exc:
            raise LLMOutputFormatError("Ollama did not return valid structured JSON.") from exc
        if not isinstance(value, dict):
            raise LLMOutputFormatError("Ollama mapping response must be a JSON object.")
        return value


class FakeLLMClient:
    """Deterministic test client; never contacts a service."""
    def __init__(self, response):
        self.response = response

    def generate(self, prompt):
        return json.dumps(self.response)

    def generate_structured(self, prompt, schema):
        return self.response
