class DPBAError(Exception):
    """Base class for actionable DPBA errors."""


class LLMResponseError(DPBAError):
    pass


class MappingValidationError(DPBAError):
    pass
