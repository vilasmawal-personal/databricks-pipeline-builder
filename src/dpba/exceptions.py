"""Actionable DPBA exception hierarchy.

Every exception class maps to a specific failure mode so callers can
distinguish recoverable from fatal issues without parsing error text.
"""


class DPBAError(Exception):
    """Base class for all DPBA errors."""


class LLMResponseError(DPBAError):
    """Ollama returned an unusable response (timeout, HTTP error, non-JSON)."""


class LLMOutputFormatError(LLMResponseError):
    """Ollama responded, but its structured output could not be decoded."""


class LLMValidationError(DPBAError):
    """Ollama returned JSON that fails PipelineSpec schema validation."""


class MappingValidationError(DPBAError):
    """The structured mapping JSON is semantically invalid."""


class InvalidMappingError(DPBAError):
    """A mapping document cannot be read or is structurally malformed."""


class UnsupportedTransformationError(DPBAError):
    """A transformation.type is not in the supported vocabulary."""


class InvalidStageConfigurationError(DPBAError):
    """A pipeline stage has missing or contradictory configuration."""


class PipelineGenerationError(DPBAError):
    """Component 2 or 3 failed to generate an artifact."""


class GeneratedArtifactError(DPBAError):
    """A generated Python or YAML artifact failed post-generation validation."""

class AmbiguousMappingError(DPBAError):
    """The pipeline spec contains ambiguous mappings that require human review."""

class UnsupportedOperationError(DPBAError):
    """The pipeline spec requests a valid operation that the current generator does not yet implement."""
