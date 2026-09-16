"""Unit tests for artifact parsing module."""

import re
from pathlib import Path
from unittest.mock import MagicMock

import orjson
import pytest

from dbt_bouncer.artifact_parsers.parser import parse_dbt_artifacts, wrap_dict
from dbt_bouncer.exceptions import DbtBouncerArtifactError


# Covers the full span of supported artifact formats: the two frozen fixtures that
# are no longer rebuilt (dbt_110, dbt_111) through to the actively built ones
# (dbt_112, dbt_20). The frozen ones guard against a parser change silently
# breaking older manifests, which nothing else would catch.
@pytest.fixture(
    params=["dbt_110", "dbt_111", "dbt_112", "dbt_20"],
    ids=["dbt_core_110", "dbt_core_111", "dbt_core_112", "dbt_20"],
)
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


def test_parse_missing_catalog_names_the_generating_commands(tmp_path):
    """A missing catalog.json must tell the user which dbt command writes one."""
    manifest = Path("tests/fixtures/dbt_20/target/manifest.json")
    (tmp_path / "manifest.json").write_bytes(manifest.read_bytes())

    bouncer_config = MagicMock()
    bouncer_config.package_name = "dbt_bouncer_test_project"
    bouncer_config.catalog_checks = [MagicMock()]
    bouncer_config.run_results_checks = []

    with pytest.raises(DbtBouncerArtifactError) as excinfo:
        parse_dbt_artifacts(bouncer_config, tmp_path)

    message = str(excinfo.value)
    assert "No catalog.json found" in message
    # dbt 2.0 moved --write-catalog off `dbt build`, so the remedy differs per line.
    assert "dbt compile --write-catalog" in message
    assert "dbt docs generate" in message
    assert "dbt-oss" in message


def _manifest_with_group_owner(owner: dict) -> dict:
    """Return the test project's manifest with a single group using `owner`.

    Args:
        owner: The `owner` mapping to attach to the group.

    Returns:
        The manifest dict, ready for `wrap_dict`.

    """
    manifest = orjson.loads(Path("dbt_project/target/manifest.json").read_bytes())
    manifest["groups"] = {
        "group.dbt_bouncer_test_project.analytics_engineering": {
            "name": "analytics_engineering",
            "resource_type": "group",
            "package_name": "dbt_bouncer_test_project",
            "path": "models/",
            "original_file_path": "models/schema.yml",
            "unique_id": "group.dbt_bouncer_test_project.analytics_engineering",
            "owner": owner,
        }
    }
    return manifest


@pytest.mark.parametrize(
    ("email", "expected"),
    [
        (
            ["user1@example.com", "user2@example.com", "user3@example.com"],
            ["user1@example.com", "user2@example.com", "user3@example.com"],
        ),
        ("single@example.com", "single@example.com"),
    ],
    ids=["list", "string"],
)
def test_group_owner_email_accepts_list_or_string(email, expected):
    """A group owner email parses whether the manifest holds a list or a string."""
    manifest = _manifest_with_group_owner(
        {"name": "Analytics Engineering Team", "email": email}
    )

    parsed_manifest = wrap_dict(manifest)

    assert (
        parsed_manifest.groups[
            "group.dbt_bouncer_test_project.analytics_engineering"
        ].owner.email
        == expected
    )
