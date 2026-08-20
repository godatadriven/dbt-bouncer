"""Unit tests for distinct CLI exit codes on config vs artifact errors."""

import json
from pathlib import Path

import yaml
from typer.testing import CliRunner

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app

runner = CliRunner()

# Resolved before any test chdirs into a tmp_path, so it stays valid throughout.
_MANIFEST_PATH = Path("./dbt_project/target/manifest.json").resolve()


def _write_config(path: Path, config: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        yaml.dump(config, f)


class TestRunExitCodes:
    """Exit codes for `dbt-bouncer run`."""

    def test_missing_config_file_exits_config_error(self, tmp_path, monkeypatch):
        """A `--config-file` pointing at a non-existent path exits CONFIG_ERROR."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("CREATE_DBT_BOUNCER_CONFIG_FILE", "false")

        result = runner.invoke(app, ["run", "--config-file", "nope.yml"])

        assert result.exit_code == ExitCode.CONFIG_ERROR

    def test_missing_manifest_exits_artifact_error(self, tmp_path, monkeypatch):
        """A config that resolves fine but has no `manifest.json` exits ARTIFACT_ERROR."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "dbt-bouncer.yml").write_text(
            "manifest_checks:\n  - name: check_model_names\n    model_name_pattern: ^[a-z]+\n"
        )

        result = runner.invoke(app, ["run"])

        assert result.exit_code == ExitCode.ARTIFACT_ERROR

    def test_invalid_only_value_exits_config_error(self, tmp_path, monkeypatch):
        """An unrecognised `--only` category exits CONFIG_ERROR, not CHECK_ERRORS."""
        monkeypatch.chdir(tmp_path)
        _write_config(
            tmp_path / "dbt-bouncer.yml",
            {
                "dbt_artifacts_dir": ".",
                "manifest_checks": [
                    {
                        "name": "check_model_names",
                        "include": "^models/staging",
                        "model_name_pattern": "^stg_",
                    }
                ],
            },
        )
        with Path.open(_MANIFEST_PATH, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        with Path.open(tmp_path / "manifest.json", "w", encoding="utf-8") as f:
            json.dump(manifest, f)

        result = runner.invoke(app, ["run", "--only", "not_a_real_category"])

        assert result.exit_code == ExitCode.CONFIG_ERROR

    def test_check_failures_exit_check_errors(self, tmp_path, monkeypatch):
        """A run where checks fail (but config/artifacts are fine) exits CHECK_ERRORS."""
        monkeypatch.chdir(tmp_path)
        _write_config(
            tmp_path / "dbt-bouncer.yml",
            {
                "dbt_artifacts_dir": ".",
                "manifest_checks": [
                    {
                        "name": "check_model_directories",
                        "include": "models",
                        "permitted_sub_directories": ["staging"],
                    },
                ],
            },
        )
        with Path.open(_MANIFEST_PATH, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        with Path.open(tmp_path / "manifest.json", "w", encoding="utf-8") as f:
            json.dump(manifest, f)

        result = runner.invoke(app, ["run"])

        assert result.exit_code == ExitCode.CHECK_ERRORS

    def test_successful_run_exits_success(self, tmp_path, monkeypatch):
        """A clean run with no failing checks exits SUCCESS."""
        monkeypatch.chdir(tmp_path)
        _write_config(
            tmp_path / "dbt-bouncer.yml",
            {
                "dbt_artifacts_dir": ".",
                "manifest_checks": [
                    {
                        "name": "check_model_names",
                        "include": "^models/staging",
                        "model_name_pattern": "^stg_",
                    }
                ],
            },
        )
        with Path.open(_MANIFEST_PATH, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        with Path.open(tmp_path / "manifest.json", "w", encoding="utf-8") as f:
            json.dump(manifest, f)

        result = runner.invoke(app, ["run"])

        assert result.exit_code == ExitCode.SUCCESS

    def test_bare_invocation_inherits_config_error_mapping(self, tmp_path, monkeypatch):
        """`dbt-bouncer` with no subcommand routes through `run` and keeps the same mapping."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("CREATE_DBT_BOUNCER_CONFIG_FILE", "false")

        result = runner.invoke(app, ["--config-file", "nope.yml"])

        assert result.exit_code == ExitCode.CONFIG_ERROR


class TestValidateExitCodes:
    """Exit codes for `dbt-bouncer validate`."""

    def test_missing_config_file_exits_config_error(self, tmp_path, monkeypatch):
        """`validate` against a missing config file exits CONFIG_ERROR."""
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["validate"])

        assert result.exit_code == ExitCode.CONFIG_ERROR
