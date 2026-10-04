"""Runtime configuration. No credentials are required for local Ollama."""
from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen3.6"
    output_dir: str = "./output"
    log_level: str = "INFO"
    default_catalog: str = "main"
    default_schema: str = "default"
    default_environment: str = "dev"
    llm_max_retries: int = 2
    llm_timeout: int = 300

    @classmethod
    def from_env(cls):
        return cls(
            ollama_base_url=os.getenv("OLLAMA_BASE_URL", cls.ollama_base_url).rstrip("/"),
            ollama_model=os.getenv("OLLAMA_MODEL", cls.ollama_model),
            output_dir=os.getenv("OUTPUT_DIR", cls.output_dir),
            log_level=os.getenv("LOG_LEVEL", cls.log_level).upper(),
            default_catalog=os.getenv("DPBA_DEFAULT_CATALOG", cls.default_catalog),
            default_schema=os.getenv("DPBA_DEFAULT_SCHEMA", cls.default_schema),
            default_environment=os.getenv("DPBA_DEFAULT_ENV", cls.default_environment),
            llm_max_retries=int(os.getenv("DPBA_LLM_MAX_RETRIES", cls.llm_max_retries)),
            llm_timeout=int(os.getenv("DPBA_LLM_TIMEOUT", cls.llm_timeout)),
        )
