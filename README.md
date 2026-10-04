# Databricks Pipeline Builder Assistant (DPBA)

The Databricks Pipeline Builder Assistant is a complete end-to-end framework for automatically designing and building data pipelines.

It leverages an LLM (such as an Ollama local model) to read a natural-language requirement document and extract a structured **Intermediate Representation (IR)**. This canonical IR is then used deterministically to:
1. Generate test data and cases.
2. Build Databricks Asset Bundles (DABs), Delta Live Tables / Job YAMLs, and PySpark notebooks.

## Architecture

```text
  Natural Language Document (Requirements)
             |
             v
+-----------------------------+
|        Component 1          |
|    (LLM Mapping Agent)      |
+-----------------------------+
             |
             v
   Canonical PipelineSpec (JSON IR)
             |
      +------+------+
      |             |
      v             v
+-----------+ +-----------+
|Component 2| |Component 3|
| Data Gen  | | DAB/Code  |
+-----------+ +-----------+
```

1. **Component 1**: Reads natural language requirements and outputs a structured PipelineSpec JSON.
2. **Component 2**: Reads the JSON, generates synthetic DataFrames, and produces exhaustive edge cases (nulls, boundary cases, etc.) based on data expectations.
3. **Component 3**: Reads the JSON and generates 100% Python-based Databricks PySpark notebooks, testing fixtures, and deployment YAMLs.

All code generation in Component 3 is **pure Python / PySpark**. Component 3 does not generate SQL files; it natively constructs Spark DataFrame operations using a deterministic transformation registry (`SnippetLibrary`).

## Local Setup with Ollama

You can run this entirely locally, ensuring zero data egress to external APIs.

### 1. Prerequisites

1. **Python 3.10+**
2. **Ollama**: Download and install from [ollama.com](https://ollama.com)
3. **Dependencies**:
   ```bash
   python -m pip install -e '.[dev]'
   ```

### 2. Prepare the LLM

Component 1 works best with models trained on structured extraction.
Start your local Ollama server, and pull a strong local model (e.g. `qwen2.5-coder:32b`, `llama3.1:8b`, or `qwen2.5:7b`):

```bash
ollama run qwen2.5-coder:7b
```

### 3. Environment Variables

Tell DPBA to point to your local Ollama instance:

```bash
export OLLAMA_BASE_URL="http://localhost:11434"
export OLLAMA_MODEL="qwen2.5-coder:7b"
export DPBA_LLM_MAX_RETRIES="3"
```

### 4. Running the Pipeline Builder

You can use the DPBA runner to execute all components end-to-end, passing in a Markdown/Text document containing the requirements:

```bash
dpba run my_pipeline_requirements.md --output ./artifacts/build
```

If you already have a structured JSON (like the provided recommended template), you can skip Component 1 (the LLM extraction phase) and directly generate code:

```bash
dpba run my_pipeline_mapping.json --output ./artifacts/build
```

### 5. Running the Tests

To ensure the framework is functioning perfectly after making modifications:

```bash
python -m pytest -v tests/
```

## Recommended Pipeline Spec JSON

The JSON format the system operates on (Canonical IR) uses strict schemas validated via Pydantic. If you want to bypass the LLM and provide the mapping manually, you should use a structure matching the `PipelineSpec` model.

See `docs/recommended_template.json` for the template.
