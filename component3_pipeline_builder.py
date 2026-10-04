"""Compatibility imports for the historical Component 3 module name."""
from dpba.component3_pipeline_builder import *  # noqa: F401,F403

if __name__ == "__main__":
    from dpba.component3_pipeline_builder import main
    raise SystemExit(main())
