"""Unit tests for the benchmark aggregation helpers.

The driver lives under ``tests/benchmark`` (not an importable package from here),
so it is loaded by file path rather than a normal import.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "benchmark" / "aggregate_benchmarks.py"
)


def _load_module():
    """Load the ``aggregate_benchmarks`` driver module by file path.

    Returns:
        The loaded module.

    """
    spec = importlib.util.spec_from_file_location("aggregate_benchmarks", _MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


agg = _load_module()


def test_parse_mean_returns_full_run_mean() -> None:
    """``_parse_mean`` returns the mean of the ``test_run_bouncer`` entry."""
    data = {
        "benchmarks": [
            {"name": "test_parse_manifest", "stats": {"mean": 0.01}},
            {"name": "test_run_bouncer", "stats": {"mean": 0.42}},
        ]
    }
    assert agg._parse_mean(data) == 0.42


def test_parse_mean_raises_when_full_run_missing() -> None:
    """``_parse_mean`` raises when no ``test_run_bouncer`` entry is present."""
    data = {"benchmarks": [{"name": "test_parse_manifest", "stats": {"mean": 0.01}}]}
    with pytest.raises(KeyError):
        agg._parse_mean(data)


def test_format_seconds() -> None:
    """``_format_seconds`` renders seconds with fixed precision, else a dash."""
    assert agg._format_seconds(0.5) == "0.5000"
    assert agg._format_seconds(12.3456789) == "12.3457"
    assert agg._format_seconds(None) == "—"


def test_parse_model_counts() -> None:
    """``_parse_model_counts`` handles defaults, spaces, and commas."""
    assert agg._parse_model_counts(None) == agg.DEFAULT_MODEL_COUNTS
    assert agg._parse_model_counts("") == agg.DEFAULT_MODEL_COUNTS
    assert agg._parse_model_counts("100 250") == [100, 250]
    assert agg._parse_model_counts("100,250,500") == [100, 250, 500]


def test_parse_model_counts_rejects_invalid() -> None:
    """``_parse_model_counts`` rejects non-integer tokens."""
    import typer

    with pytest.raises(typer.BadParameter):
        agg._parse_model_counts("100 abc")
