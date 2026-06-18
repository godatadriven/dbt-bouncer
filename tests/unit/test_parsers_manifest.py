"""Unit tests for artifact parsing module."""

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dbt_bouncer.artifact_parsers.parser import parse_dbt_artifacts


@pytest.fixture(params=["dbt_112", "dbt_20"], ids=["dbt_core_112", "dbt_20"])
def dbt_artifacts_dir(request) -> Path:
    return Path(f"tests/fixtures/{request.param}/target")


def test_parse_manifest_artifact_table_output(capsys, dbt_artifacts_dir):
    """Test that parse_dbt_artifacts outputs a table format."""
    # Load a test manifest
    bouncer_config = MagicMock()
    bouncer_config.package_name = "dbt_bouncer_test_project"
    bouncer_config.catalog_checks = []
    bouncer_config.run_results_checks = []

    # Parse all artifacts (this will trigger the table print in parser.py)
    parse_dbt_artifacts(bouncer_config, dbt_artifacts_dir)

    out = capsys.readouterr().out

    # Check that the output contains the table header and title
    assert "Parsed artifacts" in out
    assert "Category" in out
    assert "Count" in out

    # Check that the output contains expected categories
    assert "Exposures" in out
    assert "Macros" in out
    assert "Nodes" in out
    assert "Seeds" in out
    assert "Semantic Models" in out
    assert "Snapshots" in out
    assert "Sources" in out
    assert "Tests" in out
    assert "Unit Tests" in out


def test_parse_manifest_artifact_table_format(capsys, dbt_artifacts_dir):
    """Test that the table format is properly structured."""
    # Load a test manifest
    bouncer_config = MagicMock()
    bouncer_config.package_name = "dbt_bouncer_test_project"
    bouncer_config.catalog_checks = []
    bouncer_config.run_results_checks = []

    # Parse all artifacts
    parse_dbt_artifacts(bouncer_config, dbt_artifacts_dir)

    out = capsys.readouterr().out

    # Check that it contains table separators (Unicode or ASCII fallback)
    assert "---" in out or "━" in out or "─" in out

    # Check that counts are present and numeric. Rich may render the column
    # separator as either "│" (Unicode) or "|" (ASCII fallback on legacy
    # Windows consoles), so accept either.
    category_lines = re.findall(
        r"(Exposures|Macros|Nodes|Seeds|Semantic Models|Snapshots|Sources|Tests|Unit Tests).*?[│|]\s+(\d+)",
        out,
    )
    assert len(category_lines) == 9, (
        f"Expected 9 categories, found {len(category_lines)}"
    )

    # Verify all counts are numeric
    for category, count in category_lines:
        assert count.isdigit(), f"Count for {category} is not numeric: {count}"
