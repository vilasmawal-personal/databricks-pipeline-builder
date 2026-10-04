"""Runtime configuration. No credentials are required for local Ollama."""
from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen3.6"
    output_dir: str = "./output"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls):
        return cls(
            ollama_base_url=os.getenv("OLLAMA_BASE_URL", cls.ollama_base_url).rstrip("/"),
            ollama_model=os.getenv("OLLAMA_MODEL", cls.ollama_model),
            output_dir=os.getenv("OUTPUT_DIR", cls.output_dir),
            log_level=os.getenv("LOG_LEVEL", cls.log_level).upper(),
        )
