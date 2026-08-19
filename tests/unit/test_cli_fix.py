"""Unit tests for dbt_bouncer.cli.fix."""

import shutil
import sys
from pathlib import Path

from typer.testing import CliRunner

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app

runner = CliRunner()

REPO_ROOT = Path(__file__).parents[2]
ARTIFACTS_DIR = REPO_ROOT / "dbt_project" / "target"
FINANCE_YML = (
    REPO_ROOT / "dbt_project" / "models" / "marts" / "finance" / "_finance__models.yml"
)


def _write_config(tmp_path) -> Path:
    config_file = tmp_path / "dbt-bouncer.yml"
    config_file.write_text(
        f"dbt_artifacts_dir: {ARTIFACTS_DIR}\n"
        "manifest_checks:\n"
        "  - name: check_model_has_tags\n"
        "    include: ^models/marts/finance\n"
        "    tags:\n"
        "      - zz_autofix_test_tag\n"
    )
    return config_file


def _copy_project(tmp_path) -> Path:
    project_dir = tmp_path / "proj"
    target = project_dir / "models" / "marts" / "finance"
    target.mkdir(parents=True)
    shutil.copy(FINANCE_YML, target)
    return project_dir


class TestFixCommand:
    """Tests for the `fix` CLI subcommand."""

    def test_missing_dependency_exits_config_error(self, monkeypatch):
        """Without ruamel.yaml the command exits with an install hint."""
        monkeypatch.setitem(sys.modules, "ruamel.yaml", None)

        result = runner.invoke(app, ["fix"])

        assert result.exit_code == ExitCode.CONFIG_ERROR

    def test_dry_run_reports_and_writes_nothing(self, tmp_path, monkeypatch):
        """Dry run lists the fixes, exits non-zero, and leaves files alone."""
        monkeypatch.setenv("DBT_BOUNCER_DISABLE_CONF_CACHE", "1")
        config_file = _write_config(tmp_path)
        project_dir = _copy_project(tmp_path)
        yml = project_dir / "models" / "marts" / "finance" / "_finance__models.yml"
        before = yml.read_text()

        result = runner.invoke(
            app,
            [
                "fix",
                "--config-file",
                str(config_file),
                "--dbt-project-dir",
                str(project_dir),
                "--dry-run",
            ],
        )

        assert result.exit_code == ExitCode.CHECK_ERRORS
        assert "Would fix" in result.output
        assert yml.read_text() == before

    def test_fix_writes_missing_tags(self, tmp_path, monkeypatch):
        """A real run appends the missing tag to the properties file."""
        monkeypatch.setenv("DBT_BOUNCER_DISABLE_CONF_CACHE", "1")
        config_file = _write_config(tmp_path)
        project_dir = _copy_project(tmp_path)
        yml = project_dir / "models" / "marts" / "finance" / "_finance__models.yml"

        result = runner.invoke(
            app,
            [
                "fix",
                "--config-file",
                str(config_file),
                "--dbt-project-dir",
                str(project_dir),
            ],
        )

        assert result.exit_code == ExitCode.SUCCESS
        assert "Fixed" in result.output
        assert "zz_autofix_test_tag" in yml.read_text()
