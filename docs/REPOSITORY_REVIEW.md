**Databricks Pipeline Builder — repository review, 4 October 2026**

**Verdict: the intended three-component architecture exists, but the end-to-end correctness requirements are only partially met.** Component 3 is a deterministic template compiler. That property does not establish that its generated pipeline implements the mapping correctly, that its bundle preserves deployment intent, or that its integration tests exercise the generated inputs.

The supplied ChatGPT share URL could not be fetched, and no browser connection was available. This review therefore uses the user's stated expectations, repository implementation, examples, and design documents. It does not claim to incorporate the inaccessible conversation.

| Expectation | Assessment | Reason |
|---|---|---|
| Component 1 reads an abstract requirements document and creates a JSON mapping | Partial | UTF-8 text/Markdown to Ollama extraction exists, but the schema and semantic validation are too permissive; the full run does not persist the mapping. |
| Component 2 creates test data | Partial | Deterministic fixtures and useful edge cases exist, but expected values do not consistently match generated Spark behavior. |
| Component 3 deterministically builds pipeline code | Yes for generation; partial for correctness | No LLM is used; the same model produces identical text. Several valid-looking mappings generate incorrect behavior or invalid code. |
| Component 3 creates deployment YAML | Artifacts exist; configuration fidelity is incomplete | It writes `databricks.yml` and `resources/pipeline_job.yml`. The resource filename is not itself an issue. Environment and workflow information is lost or overridden. |
| Component 3 creates integration tests | A notebook exists; reliable end-to-end verification is missing | Fixtures are loaded but never fed through the pipeline automatically. Assertions miss important failure modes. |

**Actual code and execution flow**

`python -m dpba run <requirements.md> --output <directory>` is the entry point that includes all three components:

1. `dpba/__main__.py` reads the document through `read_mapping_document`. JSON files bypass the LLM.
2. `MappingAgent` combines the extraction prompt with the document, requests structured output from Ollama, checks certain forbidden keys/issues, then validates through `parse_mapping_dict`.
3. `mapping_parser.py` normalizes the legacy external mapping into `dpba/models/pipeline_spec.py`. The capability check rejects joins and unknown transformation types.
4. Component 2 generates deterministic raw rows, computes expected values across transform stages, assigns dispositions, and writes CSV/JSON fixtures.
5. Component 3 embeds fixtures into a loader notebook, emits an ingest notebook, emits one notebook per subsequent stage, emits an integration notebook, and writes the bundle/job YAML.
6. The CLI `run` command parses generated Python and YAML. It does not deploy or execute them.

`dpba_runner.py` is a different entry point: it starts from an existing mapping JSON and supports interactive reviews. It does not invoke Component 1. The README's `--requirements`, `--mapping`, and `--output-dir` examples do not match this script's positional-argument parser.

The generated Databricks job is effectively:

```text
configured source path -> ingest -> transform 1 -> ... -> final table --+
                                                                      +-> integration assertions
embedded test cases -> fixture loader -> input/expected fixture tables -+
```

There is no connection from the synthetic input fixture table to the ingest task.

**Findings, ordered by repair priority**

P1 means a correctness or execution blocker that should be resolved before relying on generated pipelines. P2 means a material reliability, configuration, or maintainability gap.

**1. [P1] Integration fixtures are disconnected from pipeline execution.**

References: `component3_pipeline_builder.py:361`, `:868`, `:887`, `:921`; `component2_data_generator.py:721`.

The fixture loader writes synthetic rows to a Unity Catalog table. Ingest independently loads `SOURCE_PATH`; it neither reads that table nor waits for fixture loading. Integration assertions then search the final production-style table for synthetic keys such as `PK-0001`. An ordinary deployment therefore fails assertions against unrelated data, or can accidentally pass against stale matching rows. Neither CSV output nor the loader populates the configured source path.

Repair: emit a separate test workflow that loads fixtures, runs the actual generated transformations using isolated test sources/targets, and asserts those outputs. Keep production execution and synthetic test execution explicit.

**2. [P1] Mapping validation accepts structurally unusable and semantically contradictory inputs.**

References: `dpba/component1.py:16`, `mapping_parser.py:66`, `:229`, `dpba/models/pipeline_spec.py:235`.

Confirmed locally: `{}` passes `parse_mapping_dict` and the agent with a fake response; Component 2 then raises `IndexError`. A response containing the required external keys and `pipeline_stages: [{}, {}]` also passes the agent. The static LLM schema has no detailed stage item schema, and the normalized model does not enforce minimum stage/source counts or meaningful mappings.

An input referencing `nonexistent_stage` passes; Component 3 instead reads the previous array element. Multiple sources, source types, first-stage operations, stage/field uniqueness, merge-key membership, parameter shapes, supported condition operators, and type validity are insufficiently checked. A declared target type `float` silently renders `StringType()`. Unknown model fields are generally ignored, so unsupported operation keys can disappear.

Repair: define one strict canonical contract, forbid unexpected fields except deliberate metadata extensions, validate supported types/parameters/references and the linear-chain restriction, and reject unsupported features before generation. Give callers actionable validation errors rather than downstream index/key errors.

**3. [P1] Component 2's expected-value calculation diverges from Spark transformations and target types.**

References: `component2_data_generator.py:143`, `:213`; `component3_pipeline_builder.py:117`, `:130`, `:160`, `:177`, `:216`.

Confirmed examples: casting raw `"001"` to integer produces expected `"001"`, while emitted Spark code casts to integer. A conditional decimal input `"1250.50"` produces expected null because the oracle uses `int(str(v))`; generated code accepts decimal input before dividing. Defaults are returned without equivalent target-type coercion. Synthetic happy values and PK overrides also assume string-friendly columns, producing invalid values for numeric keys.

The generated conditional expression casts its operand before division but does not cast its final result to the declared decimal type. Spark's division result has derived precision and scale, so the emitted schema assertion can disagree with the generated output even for the shipped monetary examples. This is a source-based inference from Spark's documented implementation, not a live cluster result. [Spark decimal arithmetic implementation](https://raw.githubusercontent.com/apache/spark/v3.5.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala).

Date-format operations with `output_format` yield strings regardless of a declared date target. Lookup and split similarly do not universally cast their final results. Conditional-date default input formats differ between the Python oracle and Spark template.

Repair: define exact typed semantics per operation, cast final Spark results, use decimal arithmetic and target coercion in the oracle, and execute parity tests against Spark. Pin/test malformed-input behavior under the supported runtime's ANSI and date-parser settings.

**4. [P1] Same-stage mappings can overwrite source columns before other mappings read them.**

References: `component3_pipeline_builder.py:412`; `component2_data_generator.py:389`.

For source `{a: "A", b: "B"}` and mappings `a <- b`, `b <- a`, Component 2 expects `{a: "B", b: "A"}`. Component 3 emits sequential `withColumn` calls: after replacing `a`, the second mapping reads the modified value, yielding both columns from original `b`. This also affects less obvious reuse of renamed or transformed input columns.

Repair: compile stage mappings as a projection over an immutable source relation, or explicitly model and validate dependencies if sequential derived-column semantics are intended.

**5. [P1] Data-quality enforcement can be skipped or disagree with fixture dispositions.**

References: `mapping_parser.py:50`; `component2_data_generator.py:286`, `:362`; `component3_pipeline_builder.py:419`, `:434`, `:447`, `:458`.

Confirmed: a `fail_pipeline` rule `account_balance BETWEEN 0 AND 100` is accepted, converted to `RAW_SQL_SHIM`, and emitted only as a comment saying it is not enforced. A build should not silently omit a requested quality rule.

Numeric-null behavior also differs. The oracle treats null as satisfying numeric comparisons. Generated filters use a condition and its negation; both evaluate to null for null operands, so a row can disappear from both valid and quarantine paths. Spark filters retain only true conditions. [Spark null semantics](https://spark.apache.org/docs/3.5.7/sql-ref-null-semantics.html).

The oracle returns on the first failed rule in declaration order; generated code always evaluates fail, then quarantine, then drop. Confirmed: a drop rule preceding a fail rule on the same null field is labeled dropped by Component 2, but the generated pipeline halts. Seeded expected outputs bypass disposition evaluation entirely. `is_nullable=False` alone is not enforced as a constraint.

Repair: reject unsupported rules, define null policy and action precedence explicitly, use the same policy in both components, and generate isolated negative jobs to verify fail-pipeline behavior.

**6. [P1] Integration assertions can pass incorrect data and fail correct typed values.**

References: `component3_pipeline_builder.py:613`, `:706`, `:737`; `component2_data_generator.py:603`, `:738`.

The notebook reads the whole final table without run filtering and retrieves only `.first()` for a single inferred key. It does not verify exact row counts, absence of unexpected rows, uniqueness, composite keys, actual quarantine contents, or that dropped records are absent. The quarantine table variable is never read. Missing/empty expected keys are silently skipped. An empty fixture list, as supplied by the CLI `build` command, results in no value assertions.

The duplicate fixture pair has identical content and both rows are marked as expected survivors; both assertions can read the same row. This cannot establish which record won, whether deduplication happened, or whether an update occurred. Intermediate checks only require a positive row count. Fail-pipeline negative cases are deliberately excluded, not tested independently.

Expected values are stored as strings and compared using `str(actual) == expected_str`. Equivalent decimals such as `1250.50` and `1250.5` fail, and empty strings are treated as null. The suite needs typed comparisons and multiset checks scoped to isolated test execution.

**7. [P1] Deployment normalization discards declared settings and environment isolation is incomplete.**

References: `mapping_parser.py:188`; `dpba/models/pipeline_spec.py:300`, `:334`, `:346`; `component3_pipeline_builder.py:802`, `:852`, `:940`.

Confirmed using the shipped customer mapping: declared targets `dev`, `staging`, `prod` become only `dev`; bundle and job names are reconstructed instead of preserved. The declared fixture `table_prefix` is ignored. Workflow parameters are stored but never emitted. Canonical environment cluster settings always resolve from the first environment. Source paths gain an unrequested `/<environment>/` suffix.

All stage catalogs are replaced with one `${var.catalog}`, even if stages intentionally name different catalogs. Conversely, quarantine writes and intermediate test queries retain hardcoded original catalog/table names. Changing deployment catalog can therefore move main writes while leaving quarantine writes or assertions in another catalog. Fixture names depend only on final stage ID and schema, so unrelated pipelines ending in `silver` can overwrite the same fixture tables. The loader uses overwrite mode.

Repair: preserve explicit mapping/environment settings, resolve every table consistently, derive quarantine targets from resolved runtime targets, and namespace test fixtures by pipeline and test run. Emit schedule/notifications only from explicit configuration; current values are hardcoded placeholders.

**8. [P1] Source ingestion can assign CSV values to the wrong columns.**

References: `dpba/models/pipeline_spec.py:266`; `component3_pipeline_builder.py:321`, `:360`.

The raw schema is derived from the first transform's mapping traversal, not an ordered source schema. With Spark CSV's default `enforceSchema=true`, the provided schema is applied positionally and headers are ignored. Reordering mappings, omitting an unmapped source column, or receiving a different CSV column order can therefore mislabel input values. This is inferred from the generated reader and Spark's documented behavior. [Spark CSV options](https://spark.apache.org/docs/3.5.7/sql-data-sources-csv.html).

A canonical table source is also accepted but generated as `.load(SOURCE_PATH)` rather than `spark.read.table`. Additional sources are unused. Repair: provide an explicit source schema/ordering and format-specific readers, or reject unsupported source configurations.

**9. [P1] Write strategy and deduplication are not faithfully implemented.**

References: `component3_pipeline_builder.py:387`, `:468`, `:523`; `dpba/models/pipeline_spec.py:126`.

Confirmed: `write_mode=overwrite` plus merge keys emits a MERGE anyway. Conversely, `merge` with no keys reaches a generic DataFrame writer mode rather than a validated upsert. Ingest also accepts merge mode but uses ordinary `.mode("merge")`. Directly running a transform notebook defaults to append regardless of the spec; the job YAML passes the mode, but standalone defaults differ.

Deduplication orders only by ingestion timestamp, which does not distinguish conflicting records from the same ingest query. There is no stable business sequence/tie-breaker. Transform stages read all rows for a date, and append stages/retries can reprocess and append earlier rows from that date. Declared schema-evolution rules are ignored while writes unconditionally enable `mergeSchema` where shown.

Repair: validate and independently implement write modes, require explicit dedup ordering, define retry/idempotency behavior, and honor or reject schema-evolution options.

**10. [P1] Valid user text can break Python/YAML generation.**

References: `component3_pipeline_builder.py:75`, `:145`, `:216`, `:277`, `:959`.

Confirmed: a concat separator containing `"` produces invalid Python; a quoted business description produces invalid YAML. Field names, formats, paths, rule names, mapping keys, and other values are interpolated directly in many places. `_python_literal` only covers some values, does not escape all control characters, and renders `None` as a string. Non-numeric condition values are interpolated as code rather than literal values.

Repair: use proper Python literal/AST rendering and YAML serialization; separately validate identifiers and paths. Apply artifact validation to every generation entry point before declaring success. The current `run` command syntax check does not establish runtime semantics or bundle-schema validity, and `build`/the legacy runner do not perform the same post-generation checks.

**11. [P1] The installable package omits required runtime files.**

Reference: `pyproject.toml:19`.

An isolated setuptools `build_py` probe using the repository configuration produced only six `dpba/*.py` modules. It omitted `dpba/models/pipeline_spec.py`, the top-level parser/generators, and prompt files. Importing the built CLI failed with `ModuleNotFoundError: No module named 'mapping_parser'`. Running tests from the repository hides this because the repository root is inserted into `sys.path`.

Repair: package the complete implementation and prompt resources, use package-relative imports, and verify an installed distribution from outside the source checkout.

**12. [P2] Component 1's ambiguity, repair, and configuration paths are inconsistent.**

References: `dpba/component1.py:52`, `:69`, `:88`, `:114`; `dpba/__main__.py:31`; `mapping_parser.py:114`.

The CLI reads environment settings but constructs `MappingAgent(llm)` without passing them, and does not pass configured timeout to `OllamaLLMClient`. Consequently configured retries/timeouts are not honored through this path. Unsupported-operation and ambiguity exceptions are outside the repair handler; a probe configured for three repairs stopped after one call with `UnsupportedOperationError`. Invalid JSON returned by the LLM also fails outside the semantic repair loop.

Manual JSON goes directly to the parser and bypasses the agent's issue/forbidden-key checks. Confirmed: a manual mapping carrying an error issue is accepted, and legacy provenance marked `human_required` becomes `high`. Low-confidence/warning mappings have no mandatory review gate on the CLI run path. Repair prompts also omit the original requirement text, limiting the context available for resolving semantic errors.

Repair: use a shared validation gate for every input route; preserve provenance, surface unresolved business ambiguity for review, distinguish repairable format errors from missing requirements, and thread settings explicitly through constructors.

**13. [P2] Full-run artifacts and documented workflows are incomplete or misleading.**

References: `dpba/__main__.py:62`, `:69`, `:83`; `dpba_runner.py:163`; `README.md`; `LOCAL_TESTING.md`; `Component_2_and_3_Deep_Dive.md`.

The `run` path does not persist Component 1's returned mapping; it parses a temporary JSON that is deleted. Only `generate` writes a mapping. This loses a central review/reproducibility artifact despite the local guide promising `pipeline_spec.json`. Input support is plain UTF-8 text/JSON; PDF/DOCX extraction is not implemented. The `test` command generates data, not pipeline execution.

The README documents incompatible legacy-runner flags and a nonexistent template location. The local guide advertises live Ollama tests, but no tests are marked `ollama`; current Component 1 tests use fakes/mocks. The deep-dive assertion that two separate implementations cannot drift is contradicted by the reproduced cases. Its integration, metadata-preservation, and fixture naming claims need updating. The standalone `fix_*.py` migration scripts mutate tests if executed and are not runtime components. Empty placeholder test files provide no coverage. Package metadata and `dpba.__version__` disagree.

Repair: persist external and normalized mappings with provenance, document one primary CLI, make command names accurately describe actions, and update design claims to match verified capability.

**Verification performed and limits**

- Reviewed runtime modules, model/parser, both entry points, prompts, tests, sample mappings, packaging, design/testing documents, and auxiliary scripts.
- Default Windows local test run: 137 passed, 6 failed. All six failures were decoding UTF-8 generated artifacts using default CP1252 reads in end-to-end tests.
- Re-run with `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` and `PYTHONUTF8=1`: **143 passed, 2 warnings**. Warnings concern an invalid Python escape in generated split code.
- Added reproducible review probes in `review/review_probes.py`; outputs are in `review/probe_results.json`. They include negative mappings, oracle mismatches, code/YAML escaping, configuration loss, package import failure, and identical-text generation checks.
- Spark, the Databricks CLI, and a connected workspace were unavailable. Generated notebooks were not executed, bundles were not validated against the Databricks CLI/schema, and no deployment was attempted. Spark-specific conclusions above are explicitly based on generated source plus primary Spark references.
- Live document extraction quality was not evaluated against Ollama; passing fake-client tests does not establish accuracy on real requirement documents.
- Application code was not modified. Only review artifacts were added.

**Recommended repair sequence and acceptance criteria**

1. Strengthen the canonical contract and shared validation gate. Every unsupported, incomplete, ambiguous, or inconsistent spec must stop before artifact generation.
2. Align typed transformation, expectation, and write semantics. Execute actual Spark tests for cast/decimal/date/null behavior, field shadowing, conflicting rules, composite keys, and deterministic deduplication.
3. Build an isolated integration workflow that feeds Component 2 inputs through the same generated code. Require exact typed output/row-count assertions, quarantine and dropped-row checks, separate expected-failure tests, and a second run verifying merge updates/idempotency.
4. Preserve environment/deployment metadata, isolate tables/fixtures, serialize safely, and validate bundles using the Databricks CLI for each target.
5. Fix distribution packaging, persist mapping artifacts, wire configuration, correct Windows text reads, and reconcile documentation with tested behavior.

The current design can support a useful single-source, linear batch-pipeline generator. Readiness should be judged by a successful isolated execution of all shipped examples and negative cases, followed by an installed-package smoke test and target-specific bundle validation; syntax-only generation tests are insufficient.
