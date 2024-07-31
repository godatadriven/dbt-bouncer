import re
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from src.dbt_bouncer.main import cli

artifact_paths = [f.__str__() for f in Path("./tests/fixtures").iterdir()]


@pytest.mark.parametrize("dbt_artifacts_dir", artifact_paths, ids=artifact_paths)
def test_cli_happy_path(caplog, dbt_artifacts_dir, tmp_path):
    with Path.open(Path("dbt-bouncer-example.yml"), "r") as f:
        bouncer_config = yaml.safe_load(f)

    bouncer_config["dbt_artifacts_dir"] = (Path(dbt_artifacts_dir) / "target").absolute().__str__()

    config_file = Path(tmp_path / "dbt-bouncer-example.yml")
    with config_file.open("w") as f:
        yaml.dump(bouncer_config, f)

    runner = CliRunner()
    result = runner.invoke(cli, f"--config-file {config_file.__str__()}")

    assert "Running dbt-bouncer (0.0.0)..." in caplog.text

    summary_count_manifest = 0
    for record in caplog.messages:
        if record.startswith("Parsed `manifest.json`, found"):
            summary_count_manifest += 1
            macros_text = re.search(r"\d* macros", record).group(0)  # type: ignore[union-attr]
            macros_num = int(re.search(r"\d*", macros_text).group(0))  # type: ignore[union-attr]
            assert macros_num > 0, f"Only found {macros_num} macros."

            nodes_text = re.search(r"\d* nodes", record).group(0)  # type: ignore[union-attr]
            nodes_num = int(re.search(r"\d*", nodes_text).group(0))  # type: ignore[union-attr]
            assert nodes_num > 0, f"Only found {nodes_num} nodes."

            sources_text = re.search(r"\d* sources", record).group(0)  # type: ignore[union-attr]
            sources_num = int(re.search(r"\d*", sources_text).group(0))  # type: ignore[union-attr]
            assert sources_num > 0, f"Only found {sources_num} sources."

            tests_text = re.search(r"\d* tests", record).group(0)  # type: ignore[union-attr]
            tests_num = int(re.search(r"\d*", tests_text).group(0))  # type: ignore[union-attr]
            assert tests_num > 0, f"Only found {tests_num} tests."

    summary_count_catalog = 0
    for record in caplog.messages:
        if record.startswith("Parsed `catalog.json`, found"):
            summary_count_catalog += 1
            nodes_text = re.search(r"\d* nodes", record).group(0)  # type: ignore[union-attr]
            nodes_num = int(re.search(r"\d*", nodes_text).group(0))  # type: ignore[union-attr]
            assert nodes_num > 0, f"Only found {nodes_num} macros."

    assert summary_count_manifest == 1
    assert summary_count_catalog == 1
    assert result.exit_code == 0


def test_cli_happy_path_pyproject_toml(caplog):
    runner = CliRunner()
    result = runner.invoke(
        cli,
    )

    assert "Loading config from pyproject.toml, if exists..." in caplog.text
    assert result.exit_code != 1


@pytest.mark.parametrize(
    "cli_args",
    [
        (""),
        ("--config-file non-existing.yml"),
    ],
)
def test_cli_unhappy_path(cli_args):
    """
    Test the happy path, just need to ensure that the CLI starts up and calculates the input parameters correctly.
    """

    runner = CliRunner()
    result = runner.invoke(
        cli,
        cli_args.split(" "),
    )
    assert result.exit_code != 0


def test_cli_config_file_doesnt_exist():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            "non-existent-file.yml",
        ],
    )
    assert type(result.exception) in [FileNotFoundError]
    assert result.exit_code != 0


def test_cli_manifest_doesnt_exist(caplog, tmp_path):
    with Path.open(Path("dbt-bouncer-example.yml"), "r") as f:
        bouncer_config = yaml.safe_load(f)

    bouncer_config["dbt_artifacts_dir"] = "non-existent-dir/target"

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
    assert result.exception.args[0].find("No manifest.json found at") == 0  # type: ignore[union-attr]
    assert result.exit_code != 0
