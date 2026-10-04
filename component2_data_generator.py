"""Compatibility imports for the historical Component 2 module name."""
from dpba.component2_data_generator import *  # noqa: F401,F403

if __name__ == "__main__":
    from dpba.component2_data_generator import main
    raise SystemExit(main())
