# Databricks Pipeline Builder Assistant (DPBA)

DPBA turns a mapping or business requirements document into a validated, deterministic Databricks Asset Bundle. Component 1 uses a local Ollama model to interpret prose as a structured mapping. Components 2 and 3 then produce test fixtures, PySpark notebooks, integration assertions, and job/DAB YAML without using an LLM.

```text
Requirements document ──> Ollama (structured mapping only) ──> validation
                                                        ├──> deterministic fixtures
                                                        └──> deterministic notebooks + DAB job
```

The model does not generate executable Python, SQL, or YAML. Ambiguity belongs in the mapping's `issues` array and should be resolved before deployment. Existing legacy `target_configurations` plus `mappings` JSON is supported.

## Components

- **Component 1:** local Ollama interpretation with a JSON Schema constrained response and strict shape validation.
- **Mapping validation:** parses the structured or legacy mapping and rejects unsupported transformations. Component 3's expectation-rule grammar is deliberately small and deterministic.
- **Component 2:** systematically creates happy-path and transformation edge cases, expected outcomes, CSV files, JSON reports, and a fixture-loading notebook.
- **Component 3:** compiles the ordered stage chain into ingest/transform notebooks, integration tests, a dynamic task DAG, and Databricks Asset Bundle YAML.

The same declarative `PipelineSpec` feeds Components 2 and 3. Component 2's expected-value functions and Component 3's Spark snippets currently live in their respective modules; tests exercise the supported semantics to guard against drift.

## Requirements and installation

Use Python 3.11 or newer. Install the package and development dependencies:

```bash
python -m pip install -e '.[dev]'
```

Install and start the company's local Ollama deployment separately, and ensure the configured model is available. DPBA never falls back to a hosted LLM.

```bash
export OLLAMA_BASE_URL="http://localhost:11434"
export OLLAMA_MODEL="qwen3.6"
```

The endpoint and model can be changed with those environment variables. `OUTPUT_DIR` and `LOG_LEVEL` are also configurable. `.env.example` documents the variables; the application reads environment variables directly and does not require a dotenv dependency.

## CLI

```bash
python -m dpba generate requirements.md --output pipeline_spec.json
python -m dpba validate pipeline_spec.json
python -m dpba build pipeline_spec.json --output ./generated/pipeline
python -m dpba test pipeline_spec.json --output ./generated
python -m dpba run requirements.md --output ./generated
```

`generate` sends only a text/Markdown input document to the configured local Ollama endpoint and writes a structured JSON mapping. Passing a JSON mapping to any command skips the model. `run` writes Component 2 output under `test_data/` and the deployable bundle under `pipeline/`, then parses all generated Python and YAML before reporting success. `build` creates the pipeline bundle; `test` creates deterministic local fixture artifacts. Existing orchestration remains available as `python dpba_runner.py mapping.json ./output --ci`.

## Mapping JSON

The preferred shape has `pipeline_metadata`, `source_configuration`, and an ordered `pipeline_stages` array. The first stage is ingestion; remaining stages are transforms. Each transform maps named source fields into target fields with a supported transformation type and typed `params`. See [`sample_specs/customer_medallion_pipeline.json`](sample_specs/customer_medallion_pipeline.json) for a three-stage example and [`sample_specs/customer_pipeline.json`](sample_specs/customer_pipeline.json) for the legacy-compatible example.

Supported deterministic transformations are `direct`, `cast`, `concat`, `conditional`, `date_format`, `conditional_date`, `lookup`, and `split`. `logic` is documentation only; it is never executed. Expectation rules support field null checks and numeric comparisons. Unsupported rules should be fixed explicitly rather than interpreted as arbitrary SQL.

The generated bundle includes `databricks.yml`, `resources/pipeline_job.yml`, and notebooks for fixture loading, ingest, each transform stage, and integration tests. Deploy from the generated bundle directory with the Databricks CLI:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run customer_pipeline_job -t dev
```

Review cloud-specific node types, workspace configuration, locations, catalog/schema permissions, and job notifications before production deployment.

## Tests and Ollama checks

```bash
python -m pytest
```

The normal suite uses `FakeLLMClient` and requires no Ollama server. Ollama connectivity can be checked by running `python -m dpba generate requirements.md`; no model call is made for JSON input. Generated notebooks are AST-parsed and generated YAML is parsed as part of `run` and in the test suite.

## Troubleshooting

- **Connection refused / model missing:** check `OLLAMA_BASE_URL`, service availability, and `OLLAMA_MODEL`; DPBA reports the local endpoint failure and stops.
- **Invalid structured mapping:** inspect required stage names, target fields, transformation names/parameters, expectation grammar, and ambiguity issues. DPBA does not repair by guessing.
- **Bundle validation fails:** check the workspace host, cloud-specific cluster node type, Unity Catalog permissions, source paths, and the generated target variables.
- **No live Databricks integration run:** local generation validates syntax and bundle structure. Executing notebooks against Unity Catalog still requires an appropriately configured Databricks workspace.
