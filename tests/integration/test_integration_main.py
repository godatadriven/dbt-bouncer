import json
import re
from pathlib import Path, PurePath

import pytest
import yaml
from click.testing import CliRunner

from dbt_bouncer.main import cli
from dbt_bouncer.utils import clean_path_str

artifact_paths = [f.__str__() for f in Path("./tests/fixtures").iterdir()]


@pytest.mark.parametrize("dbt_artifacts_dir", artifact_paths, ids=artifact_paths)
def test_cli_happy_path(caplog, dbt_artifacts_dir, tmp_path):
    with Path.open(Path("dbt-bouncer-example.yml"), "r") as f:
        bouncer_config = yaml.safe_load(f)

    bouncer_config["dbt_artifacts_dir"] = (
        (Path(dbt_artifacts_dir) / "target").absolute().__str__()
    )

    config_file = Path(tmp_path / "dbt-bouncer-example.yml")

    # Due to non-backwards compatible dbt-fusion changes, this check doesn't work with dbt-core < 1.10
    if clean_path_str(dbt_artifacts_dir).split("/")[-1] in [
        "dbt_17",
        "dbt_18",
        "dbt_19",
    ]:
        for item in bouncer_config["manifest_checks"]:
            if item["name"] == "check_source_freshness_populated":
                bouncer_config["manifest_checks"].remove(item)

    with config_file.open("w") as f:
        yaml.dump(bouncer_config, f)

    runner = CliRunner()
    result = runner.invoke(cli, f"--config-file {PurePath(config_file).as_posix()}")

    assert "Running dbt-bouncer (__version__)..." in caplog.text

    summary_count_catalog = 0
    for record in caplog.messages:
        if record.startswith("Parsed `catalog.json`"):
            summary_count_catalog += 1
            nodes_text = re.search(r"\d* nodes", record).group(0)  # type: ignore[union-attr]
            nodes_num = int(re.search(r"\d*", nodes_text).group(0))  # type: ignore[union-attr]
            assert nodes_num > 0, f"Only found {nodes_num} macros."

    summary_count_manifest = 0
    for record in caplog.messages:
        if record.startswith("Parsed `manifest.json`"):
            summary_count_manifest += 1
            exposures_text = re.search(r"\d* exposures", record).group(0)  # type: ignore[union-attr]
            exposures_num = int(re.search(r"\d*", exposures_text).group(0))  # type: ignore[union-attr]
            assert exposures_num > 0, f"Only found {exposures_num} exposures."

            macros_text = re.search(r"\d* macros", record).group(0)  # type: ignore[union-attr]
            macros_num = int(re.search(r"\d*", macros_text).group(0))  # type: ignore[union-attr]
            assert macros_num > 0, f"Only found {macros_num} macros."

            nodes_text = re.search(r"\d* nodes", record).group(0)  # type: ignore[union-attr]
            nodes_num = int(re.search(r"\d*", nodes_text).group(0))  # type: ignore[union-attr]
            assert nodes_num > 0, f"Only found {nodes_num} nodes."

            semantic_models_text = re.search(r"\d* sources", record).group(0)  # type: ignore[union-attr]
            semantic_models_num = int(re.search(r"\d*", semantic_models_text).group(0))  # type: ignore[union-attr]
            assert semantic_models_num > 0, (
                f"Only found {semantic_models_num} semantic models."
            )

            snapshots_text = re.search(r"\d* sources", record).group(0)  # type: ignore[union-attr]
            snapshots_num = int(re.search(r"\d*", snapshots_text).group(0))  # type: ignore[union-attr]
            assert snapshots_num > 0, f"Only found {snapshots_num} snapshots."

            sources_text = re.search(r"\d* sources", record).group(0)  # type: ignore[union-attr]
            sources_num = int(re.search(r"\d*", sources_text).group(0))  # type: ignore[union-attr]
            assert sources_num > 0, f"Only found {sources_num} sources."

            tests_text = re.search(r"\d* tests", record).group(0)  # type: ignore[union-attr]
            tests_num = int(re.search(r"\d*", tests_text).group(0))  # type: ignore[union-attr]
            assert tests_num > 0, f"Only found {tests_num} tests."

    summary_count_run_results = 0
    for record in caplog.messages:
        if record.startswith("Parsed `run_results.json`"):
            summary_count_run_results += 1
            nodes_text = re.search(r"\d* results", record).group(0)  # type: ignore[union-attr]
            nodes_num = int(re.search(r"\d*", nodes_text).group(0))  # type: ignore[union-attr]
            assert nodes_num > 0, f"Only found {nodes_num} macros."

    assert summary_count_manifest == 1
    assert summary_count_catalog == 1
    assert summary_count_run_results == 1
    assert result.exit_code == 0


def test_cli_coverage(caplog, tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "name": "check_model_directories",
                "include": "",
                "permitted_sub_directories": ["staging"],
            },
        ],
    }

    with Path(tmp_path / "dbt-bouncer.yml").open("w") as f:
        yaml.dump(bouncer_config, f)

    # Manifest file
    with Path.open(Path("./dbt_project/target/manifest.json"), "r") as f:
        manifest = json.load(f)

    with Path.open(tmp_path / "manifest.json", "w") as f:
        json.dump(manifest, f)

    # Run dbt-bouncer
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            Path(tmp_path / "dbt-bouncer.yml").__str__(),
            "--output-file",
            tmp_path / "coverage.json",
        ],
    )
    with Path.open(tmp_path / "coverage.json", "r") as f:
        coverage = json.load(f)

    assert (tmp_path / "coverage.json").exists()
    assert len(coverage) > 1
    assert f"Saving coverage file to `{tmp_path}/coverage.json`".replace(
        "\\", "/"
    ) in caplog.text.replace("\\", "/")
    assert (
        "`dbt-bouncer` failed. Please see below for more details or run `dbt-bouncer` with the `-v` flag."
        in caplog.text
    )
    assert result.exit_code == 1


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
    """Test the unhappy path, just need to ensure that the CLI starts up and calculates the input parameters correctly."""
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


def test_cli_error_message(caplog, tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "name": "check_model_directories",
                "include": "",
                "permitted_sub_directories": ["staging"],
            },
        ],
    }

    with Path(tmp_path / "dbt-bouncer.yml").open("w") as f:
        yaml.dump(bouncer_config, f)

    # Manifest file
    with Path.open(Path("./dbt_project/target/manifest.json"), "r") as f:
        manifest = json.load(f)

    with Path.open(tmp_path / "manifest.json", "w") as f:
        json.dump(manifest, f)

    # Run dbt-bouncer
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            Path(tmp_path / "dbt-bouncer.yml").__str__(),
        ],
    )

    assert (
        "`dbt-bouncer` failed. Please see below for more details or run `dbt-bouncer` with the `-v` flag."
        in caplog.text
    )
    assert result.exit_code == 1


def test_cli_manifest_doesnt_exist(tmp_path):
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
