"""Unit tests for dbt_bouncer.cli.studio."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from typer.testing import CliRunner

from dbt_bouncer.cli.explain.utils import get_check_name
from dbt_bouncer.cli.studio.utils import (
    filter_checks,
    load_configured_checks,
    load_run_results,
)
from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app
from dbt_bouncer.utils import get_check_objects

if TYPE_CHECKING:
    from pathlib import Path

runner = CliRunner()


class TestStudioCommand:
    """Tests for the `studio` CLI subcommand."""

    def test_studio_default_invokes_successfully(self):
        """The studio command runs and outputs header and checks table."""
        result = runner.invoke(app, ["studio"])

        assert result.exit_code == 0
        assert "dbt-bouncer studio" in result.output
        assert "Available & Configured Checks" in result.output

    def test_studio_filter_by_search(self):
        """Filtering by search shows matching checks and spotlight details."""
        result = runner.invoke(app, ["studio", "--search", "check_model_names"])

        assert result.exit_code == 0
        assert "check_model_names" in result.output
        assert "MO038" in result.output
        assert "Check Details" in result.output

    def test_studio_filter_by_category(self):
        """Filtering by category limits checks to that category."""
        result = runner.invoke(app, ["studio", "--category", "manifest_checks"])

        assert result.exit_code == 0
        assert "manifest" in result.output

    def test_studio_invalid_category_exits_config_error(self):
        """An invalid category exits with CONFIG_ERROR and lists valid choices."""
        result = runner.invoke(app, ["studio", "--category", "nonexistent_checks"])

        assert result.exit_code == ExitCode.CONFIG_ERROR
        assert "Invalid category 'nonexistent_checks'" in result.output

    def test_studio_with_config_file(self, tmp_path: Path):
        """A valid config file marks configured checks as Active."""
        config = tmp_path / "dbt-bouncer.yml"
        config.write_text(
            "manifest_checks:\n  - name: check_model_names\n    model_name_pattern: ^stg_\n"
        )

        result = runner.invoke(app, ["studio", "--config-file", str(config)])

        assert result.exit_code == 0
        assert "active" in result.output

    def test_studio_with_results_file(self, tmp_path: Path):
        """A results file displays execution status and failure counts."""
        results_file = tmp_path / "results.json"
        results_file.write_text(
            json.dumps(
                [
                    {
                        "name": "check_model_names",
                        "status": "error",
                        "severity": "error",
                    },
                    {
                        "name": "check_model_names",
                        "status": "error",
                        "severity": "error",
                    },
                ]
            )
        )

        result = runner.invoke(
            app,
            [
                "studio",
                "--results-file",
                str(results_file),
                "--search",
                "check_model_names",
            ],
            env={"COLUMNS": "160"},
        )

        assert result.exit_code == 0
        assert "Failures" in result.output
        assert "failed" in result.output

    def test_studio_missing_config_file_warns(self, tmp_path: Path):
        """An explicit config file that does not exist warns and still runs."""
        missing = tmp_path / "does_not_exist.yml"

        result = runner.invoke(app, ["studio", "--config-file", str(missing)])

        assert result.exit_code == 0
        assert "not found" in result.output

    def test_studio_no_matching_checks(self):
        """A search yielding no matches shows a clear warning panel."""
        result = runner.invoke(
            app, ["studio", "--search", "xyzzy_nonexistent_check_999"]
        )

        assert result.exit_code == 0
        assert "No checks match" in result.output


class TestStudioUtils:
    """Tests for utility functions in dbt_bouncer.cli.studio.utils."""

    def test_load_run_results_none_and_nonexistent(self, tmp_path: Path):
        """None or nonexistent path returns an empty list."""
        assert load_run_results(None) == []
        assert load_run_results(tmp_path / "does_not_exist.json") == []

    def test_load_run_results_invalid_json(self, tmp_path: Path):
        """Corrupt JSON returns an empty list."""
        corrupt = tmp_path / "corrupt.json"
        corrupt.write_text("invalid json")
        assert load_run_results(corrupt) == []

    def test_load_run_results_dict_json(self, tmp_path: Path):
        """JSON containing a dict instead of a list returns an empty list."""
        dict_json = tmp_path / "dict.json"
        dict_json.write_text(json.dumps({"status": "error"}))
        assert load_run_results(dict_json) == []

    def test_load_configured_checks_none_and_nonexistent(self, tmp_path: Path):
        """None or nonexistent path returns an empty set."""
        assert load_configured_checks(None) == set()
        assert load_configured_checks(tmp_path / "does_not_exist.yml") == set()

    def test_load_configured_checks_invalid_yaml(self, tmp_path: Path):
        """Malformed YAML file returns an empty set."""
        invalid_yaml = tmp_path / "invalid.yml"
        invalid_yaml.write_text("manifest_checks: [unclosed")
        assert load_configured_checks(invalid_yaml) == set()

    def test_load_configured_checks_toml(self, tmp_path: Path):
        """TOML configuration files are parsed correctly."""
        toml_file = tmp_path / "dbt-bouncer.toml"
        toml_file.write_text(
            '[tool.dbt-bouncer]\nmanifest_checks = [{ name = "check_model_names" }]\n'
        )
        assert load_configured_checks(toml_file) == {"check_model_names"}

    def test_load_configured_checks_invalid_toml(self, tmp_path: Path):
        """Malformed TOML file returns an empty set instead of raising."""
        invalid_toml = tmp_path / "invalid.toml"
        invalid_toml.write_text("this is = not valid toml ][")
        assert load_configured_checks(invalid_toml) == set()

    def test_filter_checks(self):
        """filter_checks filters by category and search."""
        all_checks = get_check_objects()
        filtered = filter_checks(all_checks, search="check_model_names")
        assert len(filtered) >= 1
        assert any(get_check_name(c) == "check_model_names" for c in filtered)
