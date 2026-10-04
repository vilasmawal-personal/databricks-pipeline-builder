"""Compatibility bridge to the canonical package under ``src/dpba``."""
from pathlib import Path

__path__ = [str(Path(__file__).resolve().parent.parent / "src" / "dpba")]
__version__ = "0.3.0"
