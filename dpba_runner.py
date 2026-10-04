"""Compatibility launcher for the historical ``dpba_runner.py`` script."""
from dpba.legacy_runner import cli_main

if __name__ == "__main__":
    raise SystemExit(cli_main())
