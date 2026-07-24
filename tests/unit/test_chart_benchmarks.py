"""Unit tests for the Altair benchmark chart generator helpers.

The driver lives under ``tests/benchmark`` (not an importable package from here),
so it is loaded by file path rather than a normal import.
"""

from __future__ import annotations

import importlib.util
import json

# ruff: noqa: S101
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import altair as alt
import pytest
from typer.testing import CliRunner

_MODULE_PATH = Path(__file__).resolve().parents[1] / "benchmark" / "chart_benchmarks.py"


def _load_module():
    """Load the ``chart_benchmarks`` driver module by file path.

    Returns:
        The loaded module.

    """
    spec = importlib.util.spec_from_file_location("chart_benchmarks", _MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


chart = _load_module()
runner = CliRunner()


def test_format_time() -> None:
    """``_format_time`` formats durations in seconds, ms, or μs based on magnitude."""
    assert chart._format_time(1.5) == "1.500 s"
    assert chart._format_time(0.045) == "45.00 ms"
    assert chart._format_time(0.00025) == "250.00 μs"


def test_compute_stats() -> None:
    """``_compute_stats`` calculates statistics and outlier thresholds accurately."""
    data = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10]
    stats = chart._compute_stats(data)
    assert stats["min"] == 0.01
    assert stats["max"] == 0.10
    assert stats["mean"] == pytest.approx(0.055)
    assert stats["median"] == 0.055
    assert stats["count"] == 10.0
    assert stats["stddev"] > 0.0
    assert stats["mild_lower"] < stats["q1"]
    assert stats["severe_lower"] < stats["mild_lower"]


def test_create_altair_chart() -> None:
    """``_create_altair_chart`` returns a valid layered Altair chart object."""
    timings = [0.045, 0.046, 0.044, 0.048, 0.043, 0.075]
    stats = chart._compute_stats(timings)
    altair_chart = chart._create_altair_chart("test_run_bouncer", timings, stats)
    assert isinstance(altair_chart, alt.LayerChart)
    chart_dict = altair_chart.to_dict()
    assert "layer" in chart_dict
    assert len(chart_dict["layer"]) >= 3


def test_extract_benchmarks() -> None:
    """``_extract_benchmarks`` divides raw timing samples by iterations count."""
    json_data = {
        "benchmarks": [
            {
                "name": "test_parse_manifest",
                "stats": {"data": [0.02, 0.024], "iterations": 2},
            },
            {
                "name": "test_run_bouncer",
                "stats": {"data": [0.30, 0.32], "iterations": 1},
            },
        ]
    }
    extracted = chart._extract_benchmarks(json_data)
    assert "test_parse_manifest" in extracted
    assert "test_run_bouncer" in extracted
    assert extracted["test_parse_manifest"]["data"] == [0.01, 0.012]
    assert extracted["test_run_bouncer"]["data"] == [0.30, 0.32]


def test_run_benchmark_json_failure(capsys) -> None:
    """``_run_benchmark_json`` handles subprocess failures gracefully."""
    with patch.object(chart.subprocess, "run") as mock_run:
        mock_run.return_value = SimpleNamespace(returncode=1)
        res = chart._run_benchmark_json(100, 10, "test_run_bouncer")
    assert res is None
    assert "benchmark failed (exit code 1)" in capsys.readouterr().err


def test_run_benchmark_json_passes_rounds() -> None:
    """``_run_benchmark_json`` passes ``--benchmark-min-rounds`` when specified."""
    with patch.object(chart.subprocess, "run") as mock_run:
        mock_run.return_value = SimpleNamespace(returncode=1)
        chart._run_benchmark_json(100, 10, "test_run_bouncer", benchmark_rounds=25)
        cmd = mock_run.call_args[0][0]
        assert "--benchmark-min-rounds=25" in cmd


def test_cli_with_json_file(tmp_path: Path) -> None:
    """The CLI entrypoint renders terminal scatter plot and exports Altair HTML & PNG."""
    json_path = tmp_path / "report.json"
    html_out = tmp_path / "chart.html"
    report_data = {
        "benchmarks": [
            {
                "name": "test_run_bouncer",
                "stats": {"data": [0.05, 0.052, 0.048, 0.051, 0.049], "iterations": 1},
            }
        ]
    }
    json_path.write_text(json.dumps(report_data))

    result = runner.invoke(
        chart.app,
        ["--json-file", str(json_path), "--output", str(html_out)],
    )
    assert result.exit_code == 0
    assert "Altair Performance Scatter Plot" in result.output
    assert html_out.exists()
    assert "html" in html_out.read_text().lower()
