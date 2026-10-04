"""
Shared pytest fixtures for the DPBA test suite.

Uses the installed `src/dpba` package and parametrizes
`sample_spec` / `sample_spec_path` over every committed mapping JSON in
sample_specs/ â€” so every regression test here automatically runs against
all shipped pipeline shapes (2-stage legacy, 2-stage AMEREN, 3-stage
medallion, ...) without needing to be updated when a new sample is added.
"""

from __future__ import annotations

import os
import sys

DPBA_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_SPECS_DIR = os.path.join(DPBA_DIR, "tests", "fixtures", "sample_specs")

import pytest  # noqa: E402

from dpba.compat import *  # noqa: F401,F403,E402
from dpba.mapping_parser import parse_mapping_json  # noqa: E402


def _sample_spec_paths() -> list[str]:
    return sorted(
        os.path.join(SAMPLE_SPECS_DIR, f)
        for f in os.listdir(SAMPLE_SPECS_DIR)
        if f.endswith(".json")
    )


@pytest.fixture(params=_sample_spec_paths(), ids=lambda p: os.path.basename(p))
def sample_spec_path(request) -> str:
    """Every mapping JSON under sample_specs/ â€” tests using this fixture run
    once per sample, automatically covering any new sample spec added later."""
    return request.param


@pytest.fixture
def sample_spec(sample_spec_path):
    return parse_mapping_json(sample_spec_path, verbose=False)


@pytest.fixture
def customer_spec():
    """The canonical 2-stage (legacy-shorthand) sample â€” used by tests that
    need a specific, known shape rather than running across all samples."""
    return parse_mapping_json(os.path.join(SAMPLE_SPECS_DIR, "customer_pipeline.json"), verbose=False)


def raw_pk_source(spec) -> str:
    """The RAW (stage-1) source column that ultimately becomes the final
    stage's primary key â€” mirrors DataGenerator._pk_source_fm exactly, so
    tests can independently verify PK consistency through a multi-stage
    chain without assuming primary_key_field.primary_source is a raw column
    (it's only true for 2-stage pipelines; for 3+ stages the final stage's
    PK field's source is an intermediate stage's target field name)."""
    pk_target = spec.primary_key_field.target_field
    stage1 = spec.transform_stages[0]
    for fm in stage1.fields:
        if fm.target_field == pk_target:
            return fm.primary_source
    raise AssertionError(f"primary key {pk_target!r} not found among stage 1 fields")


@pytest.fixture
def medallion_spec():
    """The 3-stage bronze->silver->gold sample â€” used by tests that
    specifically need a multi-hop chain."""
    return parse_mapping_json(
        os.path.join(SAMPLE_SPECS_DIR, "customer_medallion_pipeline.json"), verbose=False
    )

