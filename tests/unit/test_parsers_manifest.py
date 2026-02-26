"""Unit tests for parsers_manifest module."""

import logging
from pathlib import Path
from unittest.mock import MagicMock

from dbt_bouncer.artifact_parsers.parsers_common import parse_dbt_artifacts


def test_parse_manifest_artifact_table_output(caplog):
    """Test that parse_dbt_artifacts outputs a table format."""
    caplog.set_level(logging.INFO)

    # Load a test manifest
    dbt_artifacts_dir = Path("tests/fixtures/dbt_111/target")
    bouncer_config = MagicMock()
    bouncer_config.package_name = "dbt_bouncer_test_project"
    bouncer_config.catalog_checks = []
    bouncer_config.run_results_checks = []

    # Parse all artifacts (this will trigger the logging in parsers_common.py)
    parse_dbt_artifacts(bouncer_config, dbt_artifacts_dir)

    # Check that the log contains the table header
    assert any("Category" in record for record in caplog.messages)
    assert any("Count" in record for record in caplog.messages)

    # Check that the log contains expected categories
    manifest_log = next(
        record for record in caplog.messages if "Parsed artifacts" in record
    )
    assert "Exposures" in manifest_log
    assert "Macros" in manifest_log
    assert "Nodes" in manifest_log
    assert "Seeds" in manifest_log
    assert "Semantic Models" in manifest_log
    assert "Snapshots" in manifest_log
    assert "Sources" in manifest_log
    assert "Tests" in manifest_log
    assert "Unit Tests" in manifest_log


def test_parse_manifest_artifact_table_format(caplog):
    """Test that the table format is properly structured."""
    caplog.set_level(logging.INFO)

    # Load a test manifest
    dbt_artifacts_dir = Path("tests/fixtures/dbt_111/target")
    bouncer_config = MagicMock()
    bouncer_config.package_name = "dbt_bouncer_test_project"
    bouncer_config.catalog_checks = []
    bouncer_config.run_results_checks = []

    # Parse all artifacts
    parse_dbt_artifacts(bouncer_config, dbt_artifacts_dir)

    # Get the manifest log message
    manifest_log = next(
        record for record in caplog.messages if "Parsed artifacts" in record
    )

    # Check that it contains table separators
    assert "---" in manifest_log or "━" in manifest_log or "─" in manifest_log

    # Check that counts are present and numeric
    import re

    # Extract all lines that look like "│ Category        │ Count │"
    category_lines = re.findall(
        r"(Exposures|Macros|Nodes|Seeds|Semantic Models|Snapshots|Sources|Tests|Unit Tests).*?│\s+(\d+)",
        manifest_log,
    )
    assert len(category_lines) == 9, (
        f"Expected 9 categories, found {len(category_lines)}"
    )

    # Verify all counts are numeric
    for category, count in category_lines:
        assert count.isdigit(), f"Count for {category} is not numeric: {count}"
