# Pipeline Specification / IR Design (Gate 1)

## 1. Current State Analysis

### What is already good?
- **Separation of concerns:** The current JSON isolates the "transformation intent" (e.g., `{"type": "cast"}`) from the implementation. Component 3 cleanly uses this intent to generate PySpark.
- **Declarative Data Expectations:** The `drop_row`, `quarantine`, and `fail_pipeline` semantics are explicitly defined and decoupled from standard field mappings.
- **Lineage (Implicit):** `source_fields` to `target_field` establishes basic lineage.
- **Linear pipeline stages:** The `pipeline_stages` array provides a clean way to define N-stage pipelines (not hardcoded to just bronze/silver).

### What is missing?
- **Multi-source / Joins:** The current model assumes a single linear data stream (`source_configuration` at the root). It cannot express two sources joining together.
- **Aggregations / Windowing:** Missing entirely.
- **Provenance / Ambiguity:** There is no way for Component 1 to say "I don't know" or "This is a guess." The model currently forces deterministic confidence.
- **Structured Conditions:** Data expectations currently rely on raw SQL strings (e.g., `"customer_id IS NOT NULL"`). This breaks the "no code generation" rule slightly and makes it harder for Component 2 to generate Python evaluations.
- **Environment Separation:** `databricks.yml` deployment concepts (like cluster size and target environments) are mixed into the core logic via `workflow_specification`.

### What should be moved into the canonical model?
- Multiple source definitions.
- Join semantics.
- Deduplication as a first-class operation (currently it's inferred by the presence of `merge_keys` on the write operation).
- Provenance and Ambiguity tracking.

### What should NOT belong in the canonical model?
- Hardcoded Databricks concepts at the root level.
- Raw SQL strings for logic.
- Credentials or secrets.

---

## 2. Proposed Domain Model

The PipelineSpec will act as the canonical Intermediate Representation (IR). 

```text
PipelineSpec (Root)
├── metadata: PipelineMetadata
├── sources: List[SourceDefinition]
├── stages: List[PipelineStage]
├── environments: List[EnvironmentSpec]
└── tests: TestSpecification
```

### Core Entities

**1. SourceDefinition**
Supports N sources. Not all pipelines start from a single file.
- `source_id`: string (e.g., "raw_customers")
- `type`: enum (file, table, stream)
- `format`: string
- `location_ref`: string
- `read_options`: dict

**2. PipelineStage**
Represents a discrete processing step. A stage can take multiple inputs (for joins).
- `stage_id`: string
- `inputs`: List[string] (references to source_ids or previous stage_ids)
- `operations`: StageOperations
- `output`: OutputDefinition

**3. StageOperations**
Groups the logical operations that happen in a stage.
- `joins`: List[JoinDefinition]
- `mappings`: List[FieldMapping]
- `expectations`: List[DataExpectation]
- `deduplication`: DeduplicationStrategy

**4. FieldMapping & Transformation**
- `target_field`: string
- `target_type`: string
- `source_fields`: List[string]
- `transformation`: TransformationDef (type, parameters)
- `provenance`: ProvenanceMetadata (crucial for AI mapping)

**5. ProvenanceMetadata**
Tracks the AI's confidence and tracks ambiguity.
- `confidence`: enum (high, low, human_required)
- `source_reference`: string (where in the business doc did this come from?)
- `assumptions`: string

**6. OutputDefinition (Write Strategy)**
- `table_name`: string
- `write_mode`: enum (append, overwrite, merge)
- `merge_keys`: List[string]
- `schema_evolution`: SchemaEvolutionStrategy

---

## 3. Handling Ambiguity and Provenance

This is a critical addition. Since Component 1 is an AI, it MUST be able to defer to a human. If a requirement says "Map status appropriately", Component 1 should output:

```json
{
  "target_field": "status",
  "transformation": {
    "type": "UNKNOWN"
  },
  "provenance": {
    "confidence": "human_required",
    "assumptions": "Source document says 'map appropriately' but provided no mapping table."
  }
}
```

The validation layer (before Component 2/3 run) will explicitly REJECT any PipelineSpec that contains `confidence: "human_required"` or `type: "UNKNOWN"`, surfacing it to the UI/CLI so the engineer can fill in the blank. 

---

## 4. Capability Matrix

| Feature | Representable in IR? | Supported by Component 2? | Supported by Component 3? | Future? |
| :--- | :--- | :--- | :--- | :--- |
| **Linear Transforms (Cast, etc)** | Yes | Yes | Yes | - |
| **Data Expectations (Fail/Drop)** | Yes | Yes | Yes | - |
| **Delta Merges (Merge Keys)** | Yes | Yes | Yes | - |
| **Ambiguity / Provenance** | Yes (New) | N/A (Validation catches it) | N/A | - |
| **Multiple Sources (Joins)** | Yes (New) | No | No | Yes |
| **Structured Conditions (AST)** | Yes (New) | No (Uses SQL string eval) | No (Uses SQL strings) | Yes |
| **Aggregations / Windowing** | Yes (New) | No | No | Yes |
| **Schema Evolution Rules** | Yes (New) | No | No | Yes |

*Note: The IR will be designed to support Joins and Aggregations, but Components 2 and 3 do not need to implement the Python code generation for them in this immediate phase. They will gracefully ignore or throw an `UnsupportedOperationError` if asked to build a Join pipeline.*

---

## 5. Migration Plan for Existing JSONs

Because we are changing the root structure (e.g., moving `source_configuration` into an array of `sources`), existing JSON templates (like the legacy format) will fail validation.

**Strategy:**
We will introduce a `normalize_spec(raw_dict)` function in `mapping_parser.py` that runs *before* Pydantic validation. 
1. If the dict has `source_configuration` but no `sources`, it will map it to `sources: [{"source_id": "primary_input", ...}]`.
2. It will inject `inputs: ["primary_input"]` into the first pipeline stage.
3. It will default `provenance` to `{"confidence": "high"}` for all legacy fields.

This guarantees backward compatibility with the existing test suite while moving immediately to the new canonical model.
