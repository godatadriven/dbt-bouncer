"""Unit tests for parsers_manifest module."""

import logging
from pathlib import Path

import orjson

from dbt_bouncer.artifact_parsers.parsers_manifest import (
    DbtBouncerManifest,
    parse_manifest,
    parse_manifest_artifact,
)


def test_parse_manifest_artifact_table_output(caplog):
    """Test that parse_manifest_artifact outputs a table format."""
    caplog.set_level(logging.INFO)

    # Load a test manifest
    manifest_path = Path("tests/fixtures/dbt_111/target/manifest.json")
    manifest = parse_manifest(manifest=orjson.loads(manifest_path.read_bytes()))
    manifest_obj = DbtBouncerManifest(**{"manifest": manifest})

    # Parse the manifest
    parse_manifest_artifact(manifest_obj)

    # Check that the log contains the table header
    assert any("Category" in record for record in caplog.messages)
    assert any("Count" in record for record in caplog.messages)

    # Check that the log contains expected categories
    manifest_log = next(
        record for record in caplog.messages if "Parsed `manifest.json`" in record
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
    manifest_path = Path("tests/fixtures/dbt_111/target/manifest.json")
    manifest = parse_manifest(manifest=orjson.loads(manifest_path.read_bytes()))
    manifest_obj = DbtBouncerManifest(**{"manifest": manifest})

    # Parse the manifest
    parse_manifest_artifact(manifest_obj)

    # Get the manifest log message
    manifest_log = next(
        record for record in caplog.messages if "Parsed `manifest.json`" in record
    )

    # Check that it contains table separators (from tabulate)
    assert "---" in manifest_log or "━" in manifest_log or "─" in manifest_log

    # Check that counts are present and numeric
    import re

    # Extract all lines that look like "Category  Number"
    category_lines = re.findall(
        r"(Exposures|Macros|Nodes|Seeds|Semantic Models|Snapshots|Sources|Tests|Unit Tests)\s+(\d+)",
        manifest_log,
    )
    assert len(category_lines) == 9, (
        f"Expected 9 categories, found {len(category_lines)}"
    )

    # Verify all counts are numeric
    for category, count in category_lines:
        assert count.isdigit(), f"Count for {category} is not numeric: {count}"
