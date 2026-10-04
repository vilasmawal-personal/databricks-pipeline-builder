"""
DPBA — End-to-End Orchestrator
================================
Runs the full Databricks Pipeline Builder Agent workflow:

  Component 1 (assumed already done) → structured_mapping.json
       ↓
  Component 2 — Sample Data Generator Agent
       ↓  (with HITL approval)
  Component 3 — Deterministic Pipeline Builder
       ↓  (with HITL approval)
  Output: notebooks/ + DAB YAML files ready to deploy

Usage:
  python dpba_runner.py                              # uses default mapping JSON
  python dpba_runner.py path/to/mapping.json         # custom mapping JSON
  python dpba_runner.py path/to/mapping.json ./out   # custom output directory
  python dpba_runner.py mapping.json ./out --ci      # CI mode: skip HITL prompts
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import textwrap
from datetime import datetime

# Add current directory to path so sibling modules can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mapping_parser         import parse_mapping_json, print_spec_table
from component2_data_generator import run as run_data_generator
from component3_pipeline_builder import run as run_pipeline_builder


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def _banner(title: str) -> None:
    width = 70
    print("\n" + "█" * width)
    print(f"  {title}")
    print("█" * width)


def run_pipeline(
    mapping_json_path: str,
    output_root: str,
    ci_mode: bool = False,
) -> None:
    """
    Full end-to-end DPBA pipeline run.

    Args:
        mapping_json_path : Path to the structured mapping JSON (Component 1 output).
        output_root       : Root directory for all generated artifacts.
        ci_mode           : If True, skips all HITL prompts (for automated/CI runs).
    """
    start_time = datetime.now()
    _banner("DPBA — Databricks Pipeline Builder Agent")
    print(f"  Mapping JSON : {mapping_json_path}")
    print(f"  Output root  : {output_root}")
    print(f"  CI mode      : {ci_mode}")
    print(f"  Started      : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # ── Step 1: Parse mapping JSON ────────────────────────────────────────────
    _banner("Step 1 / 3 — Parse Mapping JSON")
    spec = parse_mapping_json(mapping_json_path)
    print("\n  Parsed mapping:")
    print_spec_table(spec)

    if not ci_mode:
        answer = input("\n  [HITL] Mapping looks correct? Proceed to data generation? [y/N]: ").strip().lower()
        if answer != "y":
            print("  Aborted by user at mapping review.")
            sys.exit(0)

    # ── Step 2: Component 2 — Data Generator ─────────────────────────────────
    _banner("Step 2 / 3 — Component 2: Sample Data Generator Agent")
    data_output_dir = os.path.join(output_root, "test_data")
    test_cases = run_data_generator(
        spec=spec,
        output_dir=data_output_dir,
        skip_hitl=ci_mode,
    )

    if not test_cases:
        print("  Data generation was aborted or produced no results. Stopping.")
        sys.exit(0)

    # ── Step 3: Component 3 — Pipeline Builder ────────────────────────────────
    _banner("Step 3 / 3 — Component 3: Deterministic Pipeline Builder")
    pipeline_output_dir = os.path.join(output_root, "pipeline")
    artifacts = run_pipeline_builder(
        spec=spec,
        output_dir=pipeline_output_dir,
        test_cases=test_cases,
        skip_hitl=ci_mode,
    )

    if not artifacts:
        print("  Pipeline builder was aborted. Stopping.")
        sys.exit(0)

    # ── Final summary ─────────────────────────────────────────────────────────
    elapsed = (datetime.now() - start_time).total_seconds()
    _banner("✅  DPBA Run Complete")
    print(f"\n  Completed in {elapsed:.1f}s")
    print()
    print("  Generated artifacts:")
    print(f"  ├── test_data/")
    print(f"  │     ├── synthetic_input.csv    ({len(test_cases)} rows)")
    print(f"  │     ├── expected_output.csv    ({sum(1 for t in test_cases if t.expected_disposition == 'silver')} rows)")
    print(f"  │     └── test_cases.json")
    print(f"  └── pipeline/")
    for name, path in artifacts.items():
        rel = os.path.relpath(path, output_root)
        print(f"        ├── {rel}")

    print()
    print("  ─── Next Steps ───────────────────────────────────────────────")
    print(f"  1. Review generated files in: {output_root}")
    print(f"  2. Edit pipeline/databricks.yml → set workspace_host")
    print(f"  3. Deploy:")
    print(f"       cd {pipeline_output_dir}")
    print(f"       databricks bundle validate")
    print(f"       databricks bundle deploy")
    print(f"       databricks bundle run {spec.job_name}_job")
    print(f"     (test fixtures load into Unity Catalog automatically as part of that run —")
    print(f"      {spec.test_synthetic_input_table} / {spec.test_expected_output_table} —")
    print(f"      no manual DBFS upload step)")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DPBA — Databricks Pipeline Builder Agent end-to-end runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
            Examples:
              python dpba_runner.py
              python dpba_runner.py sample_specs/ameren_billing_pipeline.json
              python dpba_runner.py mapping.json ./output
              python dpba_runner.py mapping.json ./output --ci
        """)
    )
    parser.add_argument(
        "mapping_json",
        nargs="?",
        default=None,
        help="Path to the canonical universal mapping JSON. Defaults to sample_specs/customer_pipeline.json",
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=None,
        help="Root output directory. Defaults to ./output/<timestamp>",
    )
    parser.add_argument(
        "--ci",
        action="store_true",
        help="CI mode: skip all HITL prompts and auto-approve.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))

    mapping_json = args.mapping_json or os.path.join(
        script_dir, "sample_specs", "customer_pipeline.json"
    )
    output_dir = args.output_dir or os.path.join(
        script_dir, "output", datetime.now().strftime("%Y%m%d_%H%M%S")
    )

    run_pipeline(
        mapping_json_path=os.path.abspath(mapping_json),
        output_root=os.path.abspath(output_dir),
        ci_mode=args.ci,
    )
