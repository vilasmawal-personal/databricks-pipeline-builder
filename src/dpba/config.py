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
    llm_max_retries: int = 5
    llm_timeout: int = 300
    llm_max_document_chars: int = 200_000
    llm_max_output_tokens: int = 4096
    ollama_num_ctx: int = 8192

    @classmethod
    def from_env(cls):
        retries = int(os.getenv("DPBA_LLM_MAX_RETRIES", cls.llm_max_retries))
        timeout = int(os.getenv("DPBA_LLM_TIMEOUT", cls.llm_timeout))
        document_limit = int(os.getenv("DPBA_LLM_MAX_DOCUMENT_CHARS", cls.llm_max_document_chars))
        output_tokens = int(os.getenv("DPBA_LLM_MAX_OUTPUT_TOKENS", cls.llm_max_output_tokens))
        num_ctx = int(os.getenv("OLLAMA_NUM_CTX", cls.ollama_num_ctx))
        if retries < 0:
            raise ValueError("DPBA_LLM_MAX_RETRIES must be zero or greater.")
        if timeout <= 0 or document_limit <= 0 or output_tokens <= 0 or num_ctx <= 0:
            raise ValueError("DPBA_LLM_TIMEOUT, DPBA_LLM_MAX_DOCUMENT_CHARS, DPBA_LLM_MAX_OUTPUT_TOKENS, and OLLAMA_NUM_CTX must be positive.")
        return cls(
            ollama_base_url=os.getenv("OLLAMA_BASE_URL", cls.ollama_base_url).rstrip("/"),
            ollama_model=os.getenv("OLLAMA_MODEL", cls.ollama_model),
            output_dir=os.getenv("OUTPUT_DIR", cls.output_dir),
            log_level=os.getenv("LOG_LEVEL", cls.log_level).upper(),
            default_catalog=os.getenv("DPBA_DEFAULT_CATALOG", cls.default_catalog),
            default_schema=os.getenv("DPBA_DEFAULT_SCHEMA", cls.default_schema),
            default_environment=os.getenv("DPBA_DEFAULT_ENV", cls.default_environment),
            llm_max_retries=retries,
            llm_timeout=timeout,
            llm_max_document_chars=document_limit,
            llm_max_output_tokens=output_tokens,
            ollama_num_ctx=num_ctx,
        )
