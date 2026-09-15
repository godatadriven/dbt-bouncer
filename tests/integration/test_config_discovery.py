"""Integration tests for locating config and artifacts on disk."""

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app


def test_config_discovered_from_pyproject_toml(caplog, cli_runner):
    """With no `--config-file`, the CLI falls back to `pyproject.toml`."""
    # `-v`: config discovery is logged at DEBUG, and the root logger is only
    # taken down to DEBUG when the user asks for that verbosity.
    result = cli_runner.invoke(app, ["-v"])

    assert "Loading config from pyproject.toml, if exists..." in caplog.text
    assert result.exit_code != 1


def test_missing_manifest_names_the_path(
    caplog, cli_runner, example_config, write_config
):
    """An artifacts dir with no `manifest.json` exits ARTIFACT_ERROR and says so."""
    example_config["dbt_artifacts_dir"] = "non-existent-dir/target"
    config_file = write_config(example_config, name="dbt-bouncer-example.yml")

    result = cli_runner.invoke(app, ["--config-file", str(config_file)])

    assert result.exit_code == ExitCode.ARTIFACT_ERROR
    assert "No manifest.json found at" in caplog.text
