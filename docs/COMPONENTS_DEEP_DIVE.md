# Component 2 & 3 — How DPBA Builds the Pipeline

A detailed reference for how `dpba/component2_data_generator.py` (Sample Data
Generator Agent) and `dpba/component3_pipeline_builder.py` (Deterministic
Pipeline Builder) turn one structured mapping JSON into a working, tested
Databricks pipeline. Shared infrastructure lives in `dpba/mapping_parser.py`.

---

## 1. Overview

Component 1 (AI Consultant Agent — out of scope for this document) turns a
client's messy mapping document into one structured JSON. **Everything from
that JSON onward is deterministic — no LLM writes code or test data.**

```
                    Mapping JSON
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
  Component 3 (Builder)        Component 2 (Data Gen)
          │                             │
          ▼                             ▼
  Ingest/transform         Fixture-loading notebook writes
  notebooks + job YAML     synthetic input + expected output
          │                as Unity Catalog Delta tables
          │                             │
          └──────────────┬──────────────┘
                          ▼
               Integration test notebook
          reads the UC fixture tables directly and
             validates the pipeline's real output
```

**The core design rule:** both components read the *same* per-field
`transformation.type` (one of a small fixed vocabulary — `cast`, `concat`,
`conditional`, `date_format`, `conditional_date`, `lookup`, `split`,
`direct`). Component 3 compiles each type straight to a PySpark snippet.
Component 2's "expected value" calculator (`_compute_raw_expected` in
`component2_data_generator.py`) is a **literal Python port of that same
snippet logic**, not an independently maintained guess. Because of this,
the generated test data and the generated pipeline code cannot silently
drift apart — change a transformation's semantics in one place and both
sides move together, because it's conceptually the same logic expressed
twice (once as PySpark text, once as Python arithmetic).

Component 3 is a **template compiler**: the same mapping JSON always
produces the same code. `transformation.logic` and `data_expectations[].rule`
are human-readable strings and are **never parsed to decide behavior** —
only the explicit `type` and `params` fields drive codegen. An unsupported
`transformation.type` is a loud `ValueError` at parse time, not a silent
guess.

Component 2 is **systematic, not randomized**: it enumerates the specific
edge cases each transformation type is known to be sensitive to (nulls,
boundary values, unexpected formats, enum values), rather than fuzzing
blindly.

---

## 2. The input contract — the mapping JSON

Both components consume a `PipelineSpec`, parsed by
`mapping_parser.parse_mapping_json()`. The canonical schema:

```jsonc
{
  "pipeline_metadata": {"pipeline_name": "...", "description": "...", "domain": "..."},
  "source_configuration": {
    "source_system": "...", "format": "csv", "location": "/mnt/raw/...",
    "read_options": {"header": "true", "inferSchema": "false"}
  },
  "pipeline_stages": [
    {
      "stage_name": "bronze",          // any name — not forced to "bronze"
      "stage_type": "ingest",          // always stages[0]
      "target": {"table_name": "main.bronze.customers_raw", "format": "delta", "write_mode": "append"}
    },
    {
      "stage_name": "silver",          // any name
      "stage_type": "transform",
      "target": {
        "table_name": "main.silver.customers", "format": "delta",
        "write_mode": "merge", "merge_keys": ["customer_id"]
      },
      "mappings": [
        {
          "target_field": "customer_id",
          "source_fields": ["cust_id_raw"],      // or "source_field": "x" (singular, also accepted)
          "target_data_type": "string",          // or "decimal(18,2)", "boolean", "date", "integer", ...
          "is_nullable": false,
          "default_value": null,                 // substituted via coalesce whenever the computed value is null
          "transformation": {
            "type": "cast",                       // see vocabulary table below
            "logic": "CAST(cust_id_raw AS STRING)",  // human-readable only — never parsed
            "params": {}                           // type-specific, structured, explicit
          }
        }
        // ...more field mappings
      ],
      "data_expectations": [
        {"expectation_name": "valid_customer_id", "rule": "customer_id IS NOT NULL", "action_on_failure": "fail_pipeline"}
      ]
    },
    { "stage_name": "gold", "stage_type": "transform", "...": "..." }   // optional — 0, 1, or many more hops
  ],
  "test_data_specifications": {
    "sample_inputs": [{"...": "raw row Component 1 or a human already hand-verified"}],
    "expected_outputs": [{"...": "its known-correct final-stage output"}],
    "unity_catalog": {                      // optional — where Component 2 writes its UC fixture tables (§4.8)
      "catalog": "main",                     // defaults to the ingest stage's catalog
      "schema": "dpba_test",                 // defaults to "<final_stage.schema>_test"
      "table_prefix": "customer_pipeline"    // defaults to pipeline_name
    }
  },
  "bundle_metadata": {"bundle_name": "...", "version": "1.0.0", "target_environments": ["dev", "staging", "prod"]},
  "workflow_specification": {
    "job_name": "...", "description": "...",
    "job_clusters": [{"new_cluster": {"spark_version": "...", "node_type_id": "...", "num_workers": 2, "spark_conf": {}}}],
    "parameters": {"catalog_name": "main", "schema_name": "...", "environment": "dev"}
  }
}
```

A **legacy shorthand** is still accepted for backward compatibility: a
top-level `target_configurations: {bronze_layer, silver_layer}` plus
top-level `mappings[]`/`data_expectations[]` (exactly the shape shown in the
hackathon's own reference docs). `mapping_parser._parse_stages()` detects
this and auto-upgrades it into an equivalent 2-stage `pipeline_stages`
chain internally, so nothing written against the old format broke when the
flexible N-stage model was added.

### Transformation vocabulary (`T_*` constants in `mapping_parser.py`)

| `type` | Meaning | Key `params` |
|---|---|---|
| `direct` | Passthrough cast; `F.lit(None)` if `source_fields` is empty (used for genuinely unmapped target fields) | — |
| `cast` | Type cast, trimmed if target is a string; optional case folding | `uppercase` / `lowercase` (bool) |
| `concat` | Join 2+ source fields; null **only** if *every* source is null | `separator` (default `" "`) |
| `conditional` | Numeric scale (`value / divisor`); null if non-numeric | `divisor` (default 100, or sniffed from `logic` text via `/\s*(\d+)/` as a fallback) |
| `date_format` | Parse against a list of candidate source formats; optionally re-render to `output_format` | `source_formats` (list), `output_format` |
| `conditional_date` | `date_format`, plus: raw value equal to `sentinel_value` → `NULL` (e.g. `9999-12-31` meaning "open-ended") | `source_formats`, `output_format`, `sentinel_value` |
| `lookup` | Enum value map (e.g. `{"Y": true, "N": false}`), case-insensitive by default; unmatched → `NULL` | `value_map` (dict), `case_insensitive` (bool, default true) |
| `split` | Split one source field on a delimiter, take one index | `delimiter`, `index` |

`default_value` is **universal** across every type: whatever the type's own
snippet computes (possibly `NULL`), one shared pass afterward does
`F.coalesce(computed, lit(default_value))` whenever `default_value` is set.
This is intentionally a single shared mechanism (`_apply_default()` in
Component 3, mirrored by `_expected_value()` in Component 2) rather than
default-handling duplicated inside each type's snippet — one place to get
it right, used by all eight types identically.

### Data-expectation rule grammar

`data_expectations[].rule` is **not** arbitrary SQL. `mapping_parser.parse_expectation_rule()`
is a small, bounded, deterministic regex parser recognizing only:

- `"<field> IS NOT NULL"` / `"<field> IS NULL"`
- `"<field> >= N"`, `<= `, `>`, `<`, `==`, `=`, `!=` (numeric comparisons)

A rule outside this grammar is **not guessed at** — it's reported at parse
time (`⚠️ not auto-enforceable`) and documented as a comment in the
generated code, but never silently approximated. This keeps the "no AI
guessing" guarantee intact even for free-text business rules a human wrote.

`action_on_failure` is one of three values, and it changes *where* and *how*
a violating row is handled:

| Action | Effect in the generated transform notebook |
|---|---|
| `fail_pipeline` | Checked **first**, before any write. If any row violates it, `raise Exception(...)` — the job halts, nothing is written. |
| `drop_row` | Violating rows are silently filtered out of the main dataframe (count logged). |
| `quarantine` | Violating rows are extracted into a separate dataframe, tagged with `_quarantine_reason`, and written to a `<table>_quarantine` Delta table; the pipeline continues. |

---

## 3. Component 3 — Deterministic Pipeline Builder

### 3.1 The stage-chain architecture

`PipelineSpec.stages: list[PipelineStage]` is an **ordered chain**:
`stages[0]` is always `stage_type="ingest"` (a straight copy from the raw
source, no field mappings of its own). `stages[1:]` are `stage_type="transform"`
stages, each reading the **previous** stage's table and writing its own.

This is not hardcoded to a 2-layer bronze→silver shape — it's driven
entirely by how many entries `pipeline_stages[]` has and what they're named.
`component3_pipeline_builder.run()` loops over the chain and generates:

```
notebooks/01_ingest_<stage[0].name>.py
notebooks/02_transform_<stage[1].name>.py
notebooks/03_transform_<stage[2].name>.py     ← only if a 3rd stage exists
...
notebooks/0{N+1}_integration_tests.py
```

Proven with a real 3-stage run (`sample_specs/customer_medallion_pipeline.json`,
bronze → silver → gold) and a deliberately non-medallion-named run
(`landing_zone → cleansed_zone → reporting_zone`) — both produced correctly
named, chained, syntactically valid output. See §6.

### 3.2 `generate_ingest_notebook(spec)` — stage[0]

Straight copy, no quarantine/validation logic at all (expectations operate
on *post-transformation* target fields, which don't exist yet at this
point):

1. Build a `StructType` schema from `spec.raw_source_columns` (every string,
   to avoid parse failures on ingest — typing happens downstream).
2. `spark.read.format(source_format).option(...).schema(RAW_SCHEMA).load(SOURCE_PATH)`
   — `format` and every `read_options` entry come straight from
   `source_configuration`.
3. Stamp three **audit columns**, carried through every later stage
   unchanged: `_ingested_at` (timestamp), `_source_file`, `_run_date`.
4. Write to `stages[0].full_table` using that stage's own `format`/`write_mode`.

### 3.3 `generate_transform_notebook(spec, stage_index)` — stages[1:]

This is the workhorse, generated once per transform stage. For
`stage = spec.stages[stage_index]`, `prev_stage = spec.stages[stage_index - 1]`:

1. **Read** `prev_stage`'s table, filtered to `_run_date == RUN_DATE`.
2. **Apply every field mapping** in `stage.fields`, in order — one
   `df.withColumn(...)` block per field, dispatched purely on
   `transformation_type` via `SnippetLibrary` (§3.4).
3. **Enforce data expectations**, in this exact order:
   - All `fail_pipeline` expectations first — if any violated, raise
     immediately, before touching quarantine/drop logic or any write.
   - All `quarantine` expectations — violators extracted into
     `df_quarantine`, written to `<stage_table>_quarantine`.
   - All `drop_row` expectations — violators filtered out, count logged.
   - Any expectation whose `rule` text didn't parse — emitted as a
     `# ⚠️` comment only, not enforced.
4. **Deduplicate** — *only if this stage declares `merge_keys`* (no key is
   invented when the stage doesn't specify one; rows are just appended
   as-is). When present: `Window.partitionBy(*merge_keys).orderBy(_ingested_at.desc())`,
   keep `row_number() == 1`. This step is **required** before a Delta
   `MERGE` — Delta refuses a merge whose source side has duplicate keys —
   and also protects plain-append writes from the same batch containing
   repeats.
5. **Select** this stage's target columns + the 3 audit columns (carried
   forward for the next hop to filter/dedup by).
6. **Write**:
   - `merge_keys` set → real Delta upsert:
     ```python
     if spark.catalog.tableExists(TARGET_TABLE):
         delta_target = DeltaTable.forName(spark, TARGET_TABLE)
         (delta_target.alias("t")
             .merge(df_out.alias("s"), "t.k1 = s.k1 AND ...")
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())
     else:
         df_out.write.format(...).mode("overwrite")...   # first run, table doesn't exist yet
     ```
   - No `merge_keys` → plain `.write.format(...).mode(WRITE_MODE).partitionBy("_run_date")`.

### 3.4 `SnippetLibrary` — one PySpark generator per transformation type

Each method takes a `FieldMapping` and returns a PySpark code string
(wrapped afterward by `_apply_default()`). Illustrative examples actually
produced by the builder:

**`cast`** (`customer_id` ← `cust_id_raw`, target type `string`):
```python
df = df.withColumn("customer_id", F.trim(F.col("cust_id_raw").cast(StringType())))
```

**`concat`** (`full_name` ← `first_name` + `last_name`, `default_value="UNKNOWN"`):
```python
df = df.withColumn(
    "full_name",
    F.when(F.col("first_name").isNull() & F.col("last_name").isNull(), F.lit(None).cast(StringType()))
     .otherwise(F.concat_ws(" ", F.trim(F.col("first_name").cast(StringType())), F.trim(F.col("last_name").cast(StringType()))))
)
df = df.withColumn("full_name", F.coalesce(F.col("full_name"), F.lit("UNKNOWN").cast(StringType())))
```
(The all-null check is explicit because `F.concat_ws` itself returns `""`,
not `NULL`, when every input is null — without this guard the universal
default-value coalesce below it would never fire.)

**`conditional`** (`account_balance` ← `balance_cents`, `÷100`):
```python
df = df.withColumn(
    "account_balance",
    F.when(F.col("balance_cents").cast(DecimalType(18, 2)).isNotNull(),
           F.col("balance_cents").cast(DecimalType(18, 2)) / F.lit(100))
     .otherwise(F.lit(None).cast(DecimalType(18, 2)))
)
```

**`date_format`** (`signup_date` ← `signup_dt`, 4 candidate formats):
```python
df = df.withColumn("signup_date", F.coalesce(
    F.to_date(F.col("signup_dt"), "yyyyMMdd"),
    F.to_date(F.col("signup_dt"), "yyyy/MM/dd"),
    F.to_date(F.col("signup_dt"), "yyyy-MM-dd"),
    F.to_date(F.col("signup_dt"), "MM/dd/yyyy"),
    F.lit(None).cast(DateType())
))
```

**`conditional_date`** (AMEREN-style sentinel, `service_end_datetime`):
```python
df = df.withColumn(
    "service_end_datetime",
    F.when(F.trim(F.col("Account_End_Date")) == "9999-12-31", F.lit(None).cast(StringType()))
     .otherwise(F.date_format(F.coalesce(F.to_date(F.col("Account_End_Date"), "yyyy-MM-dd"), F.lit(None).cast(DateType())), "yyyy-MM-dd'T'HH:mm:ss"))
)
```

**`lookup`** (`account_status` ← `acct_status_flag`, `{"Y": true, "N": false}`):
```python
df = df.withColumn(
    "account_status",
    F.when(F.col("acct_status_flag").isNull(), F.lit(None).cast(BooleanType()))
     .when(F.upper(F.trim(F.col("acct_status_flag"))) == "Y", F.lit(True))
     .when(F.upper(F.trim(F.col("acct_status_flag"))) == "N", F.lit(False))
     .otherwise(F.lit(None).cast(BooleanType()))
)
```

**`direct`** (unmapped target, no source column):
```python
df = df.withColumn("secondary_account_id", F.lit(None).cast(StringType()))
```

### 3.5 `generate_integration_test_notebook(spec)`

Validates the **final** stage (`spec.final_stage`) against Component 2's
test fixtures — read directly from **Unity Catalog**, not a JSON file:
`spark.read.table(spec.test_synthetic_input_table)` /
`spark.read.table(spec.test_expected_output_table)` (see §3.6a and §4.8 —
these tables are written by the generated `00_load_test_fixtures.py`,
whose *content* Component 2 generates and Component 3 assembles into the
bundle, run as an independent job task before this one):

1. **Schema conformance** — `EXPECTED_SCHEMA` is a dict built from
   `fm.simple_type_string` for every field in the final stage (e.g.
   `"decimal(18,2)"`, `"boolean"`, `"int"`) — derived from the mapping,
   never hardcoded — compared against `df.schema.fields[i].dataType.simpleString()`.
2. **Per-test-case value assertions** — for every row in the
   `expected_output` UC table (already only the `silver`-disposition
   cases — see §4.8), look up its row in the final table by the
   primary-key field (`spec.primary_key_field`, resolved from
   `merge_keys[0]`), then assert every expected column value (or null)
   matches.
3. **Declarative expectation checks** — for every `drop_row`/`quarantine`
   expectation with a parseable rule, assert its violation count in the
   live final table is exactly 0 (everything that should have been
   filtered or quarantined, was).
4. **`fail_pipeline` expectations are documented, not re-tested here** —
   deliberately. A single row violating a `fail_pipeline` rule raises
   inside the transform notebook and aborts the *entire batch* before
   anything is written — so a violating test row can never safely coexist
   with the other assertions in the same integration-test run. These are
   verified structurally instead: the generated `raise` in the transform
   notebook *is* the enforcement; the test notebook just prints a note
   pointing at it.
5. **Intermediate-stage row-count sanity checks** — for every transform
   stage *before* the final one, assert its table produced `> 0` rows for
   this run date. (Deep value assertions only make sense against the final
   stage, since that's what Component 2's `expected_output` fixture table
   was built against — see §4.)

### 3.6 `generate_databricks.yml` / `generate_job_yml` — the DAB files

`databricks.yml`: `bundle.name` from `bundle_metadata.bundle_name`; one
`targets:` block per `target_environments` entry (first is `default: true`,
`mode: development`; `prod` gets `mode: production`), each with its own
`catalog`/`env`/`source_path` variables. (There is no `test_cases_path`
bundle variable anymore — removed along with the DBFS-JSON mechanism; see
§3.6a.)

`resources/pipeline_job.yml`: the job's `tasks:` list is **built
dynamically**, one task per stage transition, chained by `depends_on` in
stage order — `_build_task_blocks()` walks `spec.stages` exactly like the
notebook generator does, so the YAML's DAG always matches the actual
notebook files on disk. Cluster spec (`spark_version`, `node_type_id`,
`num_workers`, `spark_conf`) comes from `workflow_specification.job_clusters[0]`.
`max_retries: 0` on the test task is deliberate — a failed integration test
should never silently retry and pass; it needs a human to look at it
(human-in-the-loop principle).

### 3.6a The `load_test_fixtures` task — test data lives in Unity Catalog, not DBFS

`_build_task_blocks()` always emits one extra task, `load_test_fixtures`,
running `notebooks/00_load_test_fixtures.py`. It has **no `depends_on`** —
it doesn't read anything the ingest/transform chain produces, so it runs in
parallel with `ingest_to_<stage0>` — and `run_integration_tests` depends on
**both** it and the last transform task:

```
load_test_fixtures          <- []
ingest_to_bronze             <- []
transform_to_silver          <- [ingest_to_bronze]
run_integration_tests        <- [transform_to_silver, load_test_fixtures]
```

This replaced an earlier version of the pipeline where the integration test
notebook parsed a `test_cases.json` file that had to be manually copied to
DBFS before every run (`databricks fs cp test_cases.json /dbfs/...`) — a
step outside the generated code, easy to forget, and not really "in Unity
Catalog" the way the rest of the pipeline's data is. Now the fixture data is
real Delta tables, loaded automatically as part of the job itself. See §4.8.

**Compute: `load_test_fixtures` and `run_integration_tests` get their own
minimal cluster.** `generate_job_yml()` defines a *second* `job_clusters`
entry, `test_utility_cluster` — single-node (`num_workers: 0`, driver-only,
`spark.databricks.cluster.profile: singleNode`) — and routes exactly these
two tasks to it instead of `pipeline_shared_cluster`. Neither touches real
data volume: `load_test_fixtures` writes a few dozen literal rows,
`run_integration_tests` runs counts/filters over those same small tables.
Only `ingest_to_<stage0>` and every `transform_to_<stage>` task — the ones
that actually process the pipeline's real data — use the autoscaling
`pipeline_shared_cluster` sized from `workflow_specification.job_clusters[0]`.
This is a deliberate least-compute choice: two always-cheap utility tasks
no longer provision (or scale up) a multi-worker cluster on every run.

---

## 4. Component 2 — Sample Data Generator Agent

### 4.1 Design: full fuzzing on stage 1, chain-correctness after

Exhaustive edge-case generation (nulls, boundaries, type mismatches, format
variants) runs **only against the first transform stage's fields** —
that's where real-world messy data actually enters the pipeline. Forcing a
*specific* value at a later stage (e.g. stage 3's "gold" layer) would
require inverting whatever stage 2 did to produce it, which isn't always
well-defined (you can't generically invert a `concat` or a `lookup`). This
is a deliberate, documented scope boundary — not a silent gap — confirmed
with the user before implementation.

Every test row's **expected final value** is still computed correctly for
however many stages exist, by **walking the entire chain**:
`_compute_chain_and_disposition()` applies stage 1's field mappings to the
raw synthetic input, then feeds that computed row forward as stage 2's
input, and so on, until the final stage. This is exactly how the real
generated pipeline executes (stage N reads stage N-1's output table), just
done in Python instead of Spark.

### 4.2 The expected-value mirror (`_compute_raw_expected`)

For every `transformation_type`, there's a Python function that computes
*exactly* what the matching PySpark snippet computes — this is the single
source of truth that guarantees Component 2 can never silently diverge
from Component 3:

| Type | Python mirror |
|---|---|
| `cast` | `str(v).strip()`, uppercased/lowercased per `params` |
| `concat` | `None` if every source is `None`, else `sep.join(trimmed non-null parts)` — matches `F.concat_ws`'s null-skipping exactly |
| `conditional` | `round(int(v) / divisor, 2)`, `None` on non-numeric |
| `date_format` | try each `source_formats` pattern via `datetime.strptime`, render via `output_format` if given |
| `conditional_date` | sentinel check first, then `date_format` logic |
| `lookup` | case-insensitive dict lookup against `value_map`, `None` if unmatched |
| `split` | `str(v).split(delimiter)[index]`, trimmed |
| `direct` | `str(v)` passthrough, or `None` if no source |

A tiny Spark-pattern-to-Python translator (`_spark_pattern_to_py`) converts
tokens like `yyyy-MM-dd'T'HH:mm:ss` into `%Y-%m-%dT%H:%M:%S` for both
`strptime` (parsing) and `strftime` (re-rendering) — shared by the date
types above.

`_expected_value()` wraps this with the **same universal default-value
coalesce** Component 3 applies: `None` result + `default_value` set →
return the default.

### 4.3 Happy-path synthesis (`_happy_raw`)

One plausible, well-formed raw value is synthesized per field, keyed by
`transformation_type` — e.g. `concat` picks from a name pool
(`Alice`/`Jordan`/`Priya`/... × `Smith`/`Chen`/`Nair`/...), `lookup` picks
the first key in `value_map`, `date_format` renders `2023-01-15` in the
field's own first candidate format, `cast`/`direct` synthesize a
recognizable placeholder like `CUSTOMER-0001`.

If the mapping JSON's own `test_data_specifications.sample_inputs` /
`expected_outputs` are present, those are **also** emitted as real test
cases (tagged `edge_case="seed_sample"`) — honoring the hackathon's
reference architecture, where Component 1 (or a human) can hand Component
2 pre-verified fixtures to extend, not just synthesize everything from
scratch.

### 4.4 Edge-case variants per transformation type (`_field_variant_cases`)

| Type | Variants generated |
|---|---|
| `conditional` | zero, null→default, negative, very large, non-numeric string, empty string |
| `lookup` | every declared key in both letter cases, empty value, one unrecognized value |
| `date_format` / `conditional_date` | every declared `source_formats` entry, an unparseable string, empty string, an invalid calendar date (month 13); for `conditional_date`, also the literal `sentinel_value` |
| `cast` | leading/trailing whitespace, a 255-char value (boundary), a numeric-looking string |
| `concat` | each source field null individually (checking the *other* field alone survives), all sources null, special characters (accents/apostrophes/hyphens) |
| `split` | the delimiter absent from the source string |

Plus, independent of type: a **duplicate-primary-key pair** (two identical
rows, same key — proving only the latest survives dedup/MERGE) and a
**whitespace-stress row** (every string-ish field padded at once).

### 4.5 Expectation-violation rows (`_expectation_cases`)

For every `drop_row`/`quarantine` expectation on **stage 1** with a
parseable rule, one deliberately-violating row is generated:
- `IS NOT NULL` → null out that field's source column(s)
- numeric comparisons (`>=`, `<`, etc.) → `_violating_target_value()` picks
  a value just past the boundary, then `_raw_value_for_numeric_target()`
  inverts it back to a raw input (e.g. for `conditional`, multiplies by
  the divisor to get cents from dollars)

`fail_pipeline` expectations are **never** exercised this way — see §3.5.

### 4.6 Disposition resolution — the safety net

Here's the subtlety that was actually caught as a real bug during
development: a row engineered to test *one* field can **incidentally**
violate a *different* expectation. Example: a deliberately negative
`account_balance` test row (built to test the `conditional` type's
boundary handling) also happens to violate the separate `positive_balance
>= 0` quarantine expectation — if left mislabeled, it would claim
`expected_disposition: "silver"` while the real pipeline would actually
quarantine it.

`_resolve_stage_disposition()` re-checks every computed row against
**every** expectation on its stage before finalizing disposition — not
just the one it was built to exercise. Three outcomes:
- satisfies everything → `"silver"` (reaches the next stage / final output)
- violates a `quarantine`/`drop_row` rule → relabeled `"quarantine"` /
  `"dropped"` accordingly, and its `expected_row` is cleared (nothing to
  assert in the final table)
- would violate a `fail_pipeline` rule → flagged `"_exclude_"` and
  filtered out of the final test set entirely in `generate()`, since
  shipping it would crash the whole integration-test batch

### 4.7 Primary-key uniqueness (`_unique_pk_override`)

Every independent test row needs a **distinct** primary-key value —
otherwise Silver's MERGE/dedup would collapse unrelated test rows into one,
and the integration test's per-row lookup would only ever find the last
one. `_unique_pk_override()` generates a fresh `PK-0001`, `PK-0002`, ...
value for whichever stage-1 field ultimately becomes
`spec.primary_key_field` (resolved from the final stage's `merge_keys[0]`,
assumed to be established at stage 1 and carried through unchanged — the
overwhelmingly common real-world pattern). The deliberate duplicate-PK
pair (§4.4) is the one case that explicitly opts out of this (`fresh_pk=False`).

### 4.8 Output files

| File / table | Contents |
|---|---|
| `synthetic_input.csv` | Every test case's **raw** input row (source-column-keyed), plus `_tc_id`, `_scenario`, `_edge_case`, `_expected_disposition` — local copy, for human/CI inspection |
| `expected_output.csv` | Only rows where `_expected_disposition == "silver"` — the final stage's expected column values — local copy, for human/CI inspection |
| `test_cases.json` | Full metadata per case: `tc_id`, `scenario`, `edge_case`, `expected_disposition`, `input_row`, `expected_row` — local copy, for debugging/tooling |
| `notebooks/00_load_test_fixtures.py` | A **generated Databricks notebook** (`generate_load_fixtures_notebook()`) that embeds the same `synthetic_input`/`expected_output` rows as literal data and writes them into two real **Unity Catalog Delta tables** — `spec.test_synthetic_input_table` and `spec.test_expected_output_table` (default `{catalog}.{schema}_test.{pipeline_name}_synthetic_input` / `..._expected_output`, overridable via `test_data_specifications.unity_catalog` in the mapping JSON). **This — not the CSV/JSON files above — is what the integration test notebook actually reads at runtime** (`spark.read.table(...)`, see §3.5/§3.6a). Data is embedded as literal row tuples matched against an explicit all-string `StructType`, not dict-based inference, for portability across Spark versions. |

---

## 5. End-to-end walkthrough — `account_balance`

Tracing one field mapping all the way through both components, using
`sample_specs/customer_pipeline.json`:

```jsonc
{
  "target_field": "account_balance", "source_fields": ["balance_cents"],
  "target_data_type": "decimal(18,2)", "is_nullable": true, "default_value": 0.00,
  "transformation": {"type": "conditional", "params": {"divisor": 100}}
}
```

**Component 3** emits (in `02_transform_silver.py`):
```python
df = df.withColumn(
    "account_balance",
    F.when(F.col("balance_cents").cast(DecimalType(18, 2)).isNotNull(),
           F.col("balance_cents").cast(DecimalType(18, 2)) / F.lit(100))
     .otherwise(F.lit(None).cast(DecimalType(18, 2)))
)
df = df.withColumn("account_balance", F.coalesce(F.col("account_balance"), F.lit(0.0).cast(DecimalType(18, 2))))
```
and, because of the separate `positive_balance >= 0` quarantine
expectation, also a block in the same notebook:
```python
_mask = ~(F.col("account_balance") >= 0.0)
_q_parts.append(df.filter(_mask).withColumn("_quarantine_reason", F.lit("positive_balance")))
df = df.filter(~_mask)
```
and the schema check in `03_integration_tests.py` gets
`"account_balance": "decimal(18,2)"` in `EXPECTED_SCHEMA`.

**Component 2** generates, among others, these rows for this field
(`_field_variant_cases`, `T_CONDITIONAL` branch):

| Scenario | Raw `balance_cents` | `_compute_raw_expected` | Final disposition |
|---|---|---|---|
| zero boundary | `"0"` | `0.0` | `silver` |
| null → default | `None` | `None` → default `0.0` | `silver` |
| negative value | `"-500"` | `-5.0` | **`quarantine`** (caught by §4.6's cross-check against `positive_balance >= 0`, even though this row was built to test the `conditional` type, not the expectation) |
| large value | `"999999900"` | `9999999.0` | `silver` |
| non-numeric | `"abc"` | `None` → default `0.0` | `silver` |
| empty string | `""` | `None` → default `0.0` | `silver` |

The negative-value row is the concrete proof of why §4.6 exists: without
it, that row would have been shipped as an expected-`silver` row with
`account_balance = -5.00`, which the real pipeline would never actually
produce (it quarantines negative balances) — the test would have asserted
something false.

---

## 6. Flexibility — proof the architecture isn't hardcoded to medallion

Two concrete runs prove `pipeline_stages[]` genuinely drives everything,
not just table names:

**3-stage medallion** (`sample_specs/customer_medallion_pipeline.json`,
bronze → silver → gold): produced `01_ingest_bronze.py`,
`02_transform_silver.py`, `03_transform_gold.py`, `04_integration_tests.py`,
a job DAG `ingest_to_bronze → transform_to_silver → transform_to_gold →
run_integration_tests`, and test data whose expected values correctly
chain through *two* hops (`first_name`+`last_name` → silver's `full_name`
via `concat` → gold's `display_name` via uppercased `cast`).

**Non-medallion naming** (a throwaway test: `landing_zone → cleansed_zone →
reporting_zone`): produced `01_ingest_landing_zone.py`,
`02_transform_cleansed_zone.py`, `03_transform_reporting_zone.py`,
correctly chained, all valid Python/YAML. Grepping both generator modules
for the literal strings `"bronze"`/`"silver"`/`"gold"` turns up nothing
except the legacy-shorthand parser (which *is* that named format, by
definition) and docstring examples — no actual codegen logic branches on
those names.

**Known limitation:** the chain is strictly **linear**. Each stage has
exactly one predecessor (the previous stage in the list), and the ingest
stage has exactly one source. Branching shapes (two raw sources merging
into one stage, or one stage fanning out to two parallel downstream
stages) are not supported — that would need a stage to declare multiple
named inputs, which isn't implemented.

---

## 7. Quick reference

### `mapping_parser.py`
- `FieldMapping` — one field's source/target/type/transformation
- `DataExpectation` — one parsed rule + its action
- `PipelineStage` — one hop (`ingest` or `transform`)
- `PipelineSpec` — the whole chain + source config + bundle/workflow config
  + `test_catalog`/`test_schema`/`test_table_prefix` and the
  `test_synthetic_input_table`/`test_expected_output_table` properties
  (Unity Catalog fixture table names, §4.8)
- `parse_mapping_json()` — entry point; handles both schema forms
- `parse_expectation_rule()` — the bounded rule-grammar parser
- `classify`/`T_*` constants — the transformation vocabulary

### `component3_pipeline_builder.py`
- `SnippetLibrary` — one PySpark generator per `transformation_type`
- `_apply_default()` — universal default-value coalesce pass
- `_spark_condition()` — expectation rule → Spark boolean expression
- `generate_ingest_notebook()`, `generate_transform_notebook()`,
  `generate_integration_test_notebook()` — the three notebook-generator kinds
  (it also calls `component2_data_generator.generate_load_fixtures_notebook()`
  for the fourth, `00_load_test_fixtures.py` — content generated by
  Component 2, assembled here)
- `generate_databricks_yml()`, `generate_job_yml()`,
  `_build_task_blocks()` — the DAB bundle + dynamically-chained job DAG
  (includes the independent `load_test_fixtures` task, §3.6a)
- `run(spec, output_dir, test_cases, skip_hitl)` — orchestrates the whole
  stage loop, writes every file; `test_cases` is Component 2's generated
  `TestCase` list, needed to render the fixtures notebook

### `component2_data_generator.py`
- `_compute_raw_expected()` / `_expected_value()` — the Python mirror of
  `SnippetLibrary`, per type
- `_happy_raw()` — synthesizes one valid value per type
- `DataGenerator` — the test-case engine:
  - `_compute_chain_and_disposition()` — walks the full stage chain
  - `_resolve_stage_disposition()` — checks one stage's expectations
  - `_field_variant_cases()` / `_concat_variant_cases()` — per-type edge cases
  - `_expectation_cases()` — deliberate expectation-violating rows
  - `_duplicate_cases()`, `_whitespace_case()` — cross-cutting stress tests
- `write_synthetic_input_csv()`, `write_expected_output_csv()`,
  `write_test_cases_json()` — local CSV/JSON copies, for inspection/CI
- `generate_load_fixtures_notebook(spec, tcs)` — **the function that
  actually matters at runtime**: renders `00_load_test_fixtures.py`,
  embedding the same rows as literal data and writing them into the two
  Unity Catalog tables the integration test notebook reads (§3.5, §4.8)

### Generated output layout
```
output/
├── test_data/                      ← local copies, for inspection/CI only
│   ├── synthetic_input.csv
│   ├── expected_output.csv
│   └── test_cases.json
└── pipeline/
    ├── notebooks/
    │   ├── 00_load_test_fixtures.py        ← writes the UC fixture tables
    │   ├── 01_ingest_<stage0>.py
    │   ├── 02_transform_<stage1>.py
    │   ├── ...
    │   └── 0{N+1}_integration_tests.py     ← reads the UC fixture tables
    ├── resources/pipeline_job.yml
    └── databricks.yml
```

---

## 8. Deploying to a real Databricks workspace

Everything above has been validated *statically* (`ast.parse`, `yaml.safe_load`,
and the regression suite in §9) — never actually executed on a live cluster.
The following were found by re-reading the generated code specifically
asking "will this actually run," and are now either fixed in codegen or are
real prerequisites only you can satisfy.

**Fixed in codegen this round:**
- **Schema auto-creation.** Every write point (`01_ingest_*`, every
  `0N_transform_*`, and `00_load_test_fixtures`) now runs
  `spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{catalog}}.{{schema}}")` before
  its first `saveAsTable`. Without this, a genuinely fresh Unity Catalog
  catalog (no schemas yet) would fail the very first run with a
  schema-not-found error — `saveAsTable` creates tables, never schemas.
  The **catalog** itself is deliberately *not* auto-created — that needs
  metastore-admin privileges this generated code doesn't assume; it must
  already exist.
- **Multi-environment catalog substitution for test fixtures.** The job
  YAML's `base_parameters` for `load_test_fixtures` and
  `run_integration_tests` now use `${var.catalog}` (same mechanism the
  main pipeline tables already used), so `databricks bundle deploy -t
  staging/prod` with an overridden `catalog` variable correctly repoints
  *every* task — previously the fixture tables stayed pinned to the
  dev-resolved catalog regardless of deploy target. If
  `test_data_specifications.unity_catalog.catalog` was explicitly set to a
  *different* catalog than the pipeline's own (e.g. a shared cross-pipeline
  test catalog), that override is respected as a literal and intentionally
  *not* re-pointed per environment.
- **`node_type_id` cloud-portability warning.** The generated job YAML now
  carries a loud comment next to `node_type_id` — the default
  (`m5d.large`) is an AWS-only EC2 instance type.

**Real prerequisites — not fixable in generated code, yours to satisfy:**
1. **`workspace_host`** in `databricks.yml` is a placeholder
   (`https://your-workspace.azuredatabricks.net`) — edit it before `databricks
   bundle deploy` (already called out in `dpba_runner.py`'s printed next steps).
2. **`node_type_id`** must be a valid instance type for your cloud/region —
   override it via `workflow_specification.job_clusters[0].new_cluster.node_type_id`
   in the mapping JSON if you're not on AWS, or `databricks bundle validate`
   (or the job run) will fail with an invalid-node-type error.
3. **Unity Catalog permissions.** The identity running the bundle deploy/job
   needs `USE CATALOG` + `CREATE SCHEMA` + `CREATE TABLE` on the target
   catalog (and on the test-fixture catalog, if different) — a generated
   notebook can't grant itself privileges it doesn't have.
4. **Real source data.** `source_configuration.location` (default
   `/mnt/raw/<pipeline_name>/`) must resolve to something readable in your
   workspace — a legacy DBFS mount, a Unity Catalog Volume
   (`/Volumes/catalog/schema/volume/...`), or a cloud URI
   (`s3://`/`abfss://`/`gs://`) with the cluster's instance profile/service
   principal granted access. Nothing here creates that path or the data in
   it — only `00_load_test_fixtures.py`'s synthetic rows are self-contained.
5. **The `_quarantine` table only appears once something is actually
   quarantined.** `generate_transform_notebook()`'s quarantine write is
   inside `if _q_parts:` — on a run where no row violates a `quarantine`
   expectation, the `<table>_quarantine` table is simply never created.
   Expected behavior, not a bug, but worth knowing before you go looking
   for it after a clean run.

---

## 9. Regression test suite (`dpba/tests/`)

134 pytest tests (`cd dpba && python3 -m pytest`, needs `pip install pytest
pyyaml`) lock in the guarantees described above so a future change can't
silently reintroduce a historical bug. Parametrized over every mapping JSON
in `sample_specs/`, so a new sample added later is automatically covered.

| File | What it locks in |
|---|---|
| `test_mapping_parser.py` | Legacy-shorthand auto-upgrade, the bounded expectation-rule grammar, primary-key resolution, `FieldMapping` type rendering, the loud error on an unsupported `transformation.type`, Unity Catalog fixture-table naming |
| `test_component2_data_generator.py` | **The historical bugs, directly**: `TestPrimaryKeyConsistency` (the PK off-by-one bug — a silver row's input and expected PK must match) and `TestDispositionResolution` (the cross-expectation bug — a row can't be mislabeled `silver` when it actually violates some *other* expectation); plus the expected-value mirror, generated-case sanity, and writer round-trips |
| `test_component3_pipeline_builder.py` | Per-type `SnippetLibrary` codegen, universal default handling, **every generated notebook/YAML is syntactically valid** (the check that would have caught the historical f-string reindent bug), job-DAG shape, least-compute cluster routing, Delta `MERGE` presence, schema auto-creation presence, schema-conformance assertions matching codegen exactly, and the multi-environment `${var.catalog}` substitution |
| `test_end_to_end.py` | Full Component 2 → Component 3 run per sample into a real tmp directory — on-disk file count, PK consistency via the actual written CSVs (the exact check that originally caught the PK bug by hand), and that the job YAML's notebook paths match files actually written to disk |

A key design choice: `raw_pk_source()` in `conftest.py` mirrors
`DataGenerator._pk_source_fm`'s own chain-tracing logic rather than
assuming `primary_key_field.primary_source` is a raw CSV column — that
assumption only holds for 2-stage pipelines; for a 3+-stage chain the final
stage's PK field's source is an *intermediate* stage's target field name.
(This was caught by the suite itself on first run, against the 3-stage
medallion sample — a good demonstration of exactly the kind of regression
it's meant to catch.)
