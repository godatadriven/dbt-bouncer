import pytest
from click.testing import CliRunner

from dbt_bouncer.main import cli


@pytest.mark.parametrize(
    "cli_args",
    [
        ("--config-file dbt-bouncer-example.yml --dbt-artifacts-dir dbt_project/target"),
    ],
)
def test_cli_happy_path(caplog, cli_args):
    """
    Test the happy path, just need to ensure that the CLI starts up and calculates the input parameters correctly.
    """

    runner = CliRunner()
    result = runner.invoke(
        cli,
        cli_args.split(" "),
    )
    assert "Running dbt-bouncer (0.0.0)..." in caplog.text
    assert "Loading manifest.json from dbt_project/target/manifest.json..." in caplog.text
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "cli_args",
    [
        (""),
        ("--config-file dbt-bouncer-example.yml"),
        ("--dbt-artifacts-dir dbt_project/target"),
        ("--config-file non-existing.yml --dbt-artifacts-dir dbt_project/target"),
        ("--config-file dbt-bouncer-example.yml --dbt-artifacts-dir dbt_project"),
    ],
)
def test_cli_unhappy_path(caplog, cli_args):
    """
    Test the happy path, just need to ensure that the CLI starts up and calculates the input parameters correctly.
    """

    runner = CliRunner()
    result = runner.invoke(
        cli,
        cli_args.split(" "),
    )
    assert result.exit_code != 0


def test_cli_dbt_dir_doesnt_exist():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            "dbt-bouncer-example.yml",
            "--dbt-artifacts-dir",
            "non-existent-directory/target",
        ],
    )
    assert type(result.exception) in [SystemExit]
    assert result.exit_code != 0


def test_cli_config_doesnt_exist(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            "non-existent-file.yml",
            "--dbt-artifacts-dir",
            "dbt_project/target",
        ],
    )
    assert type(result.exception) in [SystemExit]
    assert result.exit_code != 0


def test_cli_manifest_doesnt_exist(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            "dbt-bouncer-example.yml",
            "--dbt-artifacts-dir",
            tmp_path,
        ],
    )
    assert type(result.exception) in [FileNotFoundError]
    assert result.exception.args[0] == f"No manifest.json found at {tmp_path / 'manifest.json'}."
    assert result.exit_code != 0
