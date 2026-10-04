"""`python -m dpba` command line."""
import argparse
import ast
import json
import logging
from pathlib import Path
import sys
import tempfile
import yaml

from dpba.component1 import MappingAgent, read_mapping_document
from dpba.config import Settings
from dpba.exceptions import DPBAError
from dpba.llm import OllamaLLMClient
from dpba.mapping_parser import parse_mapping_json
from dpba.component2_data_generator import run as generate_data
from dpba.component3_pipeline_builder import run as build_pipeline
from dpba import compat as _compat  # noqa: F401 -- register temporary old module aliases


def _parser():
    parser = argparse.ArgumentParser(prog="dpba", description="Deterministic Databricks pipeline generator")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("generate", "validate", "build", "test", "run"):
        p = sub.add_parser(command)
        p.add_argument("input", help="Mapping JSON or requirement document")
        p.add_argument("--output", default=None, help="Output directory")
        p.add_argument("--model", default=None, help="Local Ollama model name")
    return parser


def _obtain_mapping(input_path, settings, model):
    value = read_mapping_document(input_path)
    if isinstance(value, str):
        print("[2/6] Interpreting requirements with local Ollama")
        llm = OllamaLLMClient(
            settings.ollama_base_url,
            model or settings.ollama_model,
            timeout=settings.llm_timeout,
            max_output_tokens=settings.llm_max_output_tokens,
            num_ctx=settings.ollama_num_ctx,
        )
        value = MappingAgent(llm, settings=settings).generate(value)
    return value


def _parse_mapping(raw):
    with tempfile.TemporaryDirectory(prefix="dpba-") as temp:
        path = Path(temp) / "mapping.json"
        path.write_text(json.dumps(raw), encoding="utf-8")
        return parse_mapping_json(str(path), verbose=False)


def _validate_artifacts(output):
    for path in Path(output).rglob("*.py"):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for path in Path(output).rglob("*.yml"):
        yaml.safe_load(path.read_text(encoding="utf-8"))
    for path in Path(output).rglob("*.yaml"):
        yaml.safe_load(path.read_text(encoding="utf-8"))


def main(argv=None):
    settings = Settings.from_env()
    args = _parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, settings.log_level, logging.INFO), format="%(levelname)s %(message)s")
    output = Path(args.output or settings.output_dir)
    try:
        print("[1/6] Reading mapping document")
        raw = _obtain_mapping(args.input, settings, args.model)
        if args.command == "generate":
            output.parent.mkdir(parents=True, exist_ok=True)
            output = Path(args.output or "pipeline_spec.json")
            output.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
            print("Wrote {}".format(output))
            return 0
        if args.command in {"build", "test", "run"}:
            output.mkdir(parents=True, exist_ok=True)
            mapping_artifact = output / "pipeline_spec.json"
            mapping_artifact.write_text(json.dumps(raw, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
            print("Saved validated mapping to {}".format(mapping_artifact))
        print("[3/6] Validating mapping")
        spec = _parse_mapping(raw)
        if args.command == "validate":
            print("Valid: {} stage(s), {} transform mapping(s)".format(len(spec.stages), sum(len(s.fields) for s in spec.transform_stages)))
            return 0
        if args.command == "build":
            cases = []
            build_pipeline(spec, str(output), cases, skip_hitl=True)
        elif args.command == "test":
            cases = generate_data(spec, str(output / "test_data"), skip_hitl=True)
            print("Generated {} deterministic cases".format(len(cases)))
            return 0
        else:
            print("[4/6] Generating deterministic test fixtures")
            cases = generate_data(spec, str(output / "test_data"), skip_hitl=True)
            print("[5/6] Generating Databricks pipeline")
            build_pipeline(spec, str(output / "pipeline"), cases, skip_hitl=True)
            print("[6/6] Validating generated Python and YAML")
            _validate_artifacts(output / "pipeline")
            print("Ready: {}".format(output))
        return 0
    except (DPBAError, ValueError, OSError, json.JSONDecodeError, yaml.YAMLError, SyntaxError) as exc:
        logging.error("%s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
