from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from dbt_bouncer.main import cli


@pytest.mark.parametrize(
    "cli_args",
    [
        ("--config-file dbt-bouncer-example.yml"),
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
        ("--config-file non-existing.yml"),
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


def test_cli_config_doesnt_exist(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            "non-existent-file.yml",
        ],
    )
    assert type(result.exception) in [SystemExit]
    assert result.exit_code != 0


def test_cli_manifest_doesnt_exist(tmp_path):
    with Path.open(Path("dbt-bouncer-example.yml"), "r") as f:
        bouncer_config = yaml.safe_load(f)

    bouncer_config["dbt-artifacts-dir"] = "non-existent-dir/target"

    with Path(tmp_path / "dbt-bouncer-example.yml").open("w") as f:
        yaml.dump(bouncer_config, f)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            Path(tmp_path / "dbt-bouncer-example.yml").__str__(),
        ],
    )
    assert type(result.exception) in [FileNotFoundError]
    assert (
        result.exception.args[0]  # type: ignore[union-attr]
        == "No manifest.json found at non-existent-dir/target/manifest.json."
    )
    assert result.exit_code != 0
