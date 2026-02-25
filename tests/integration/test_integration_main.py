import json
import re
from pathlib import Path, PurePath

import pytest
import yaml
from typer.testing import CliRunner

from dbt_bouncer.main import app
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

    # These checks doesn't work with dbt-core < 1.9
    if clean_path_str(dbt_artifacts_dir).split("/")[-1] in ["dbt_17", "dbt_18"]:
        for item in bouncer_config["catalog_checks"]:
            if item["name"] in ["check_seed_columns_are_all_documented"]:
                bouncer_config["catalog_checks"].remove(item)
        for item in bouncer_config["manifest_checks"]:
            if item["name"] in ["check_seed_description_populated"]:
                bouncer_config["manifest_checks"].remove(item)

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
    result = runner.invoke(app, f"--config-file {PurePath(config_file).as_posix()}")

    assert "Running dbt-bouncer (0.0.0)..." in caplog.text

    summary_count_artifacts = 0
    for record in caplog.messages:
        if record.startswith("Parsed artifacts"):
            summary_count_artifacts += 1
            # The record now contains a table format
            # Extract counts using regex for each category
            exposures_match = re.search(r"Exposures.*?│\s+(\d+)", record)
            assert exposures_match, "Could not find Exposures in table"
            exposures_num = int(exposures_match.group(1))
            assert exposures_num > 0, f"Only found {exposures_num} exposures."

            macros_match = re.search(r"Macros.*?│\s+(\d+)", record)
            assert macros_match, "Could not find Macros in table"
            macros_num = int(macros_match.group(1))
            assert macros_num > 0, f"Only found {macros_num} macros."

            nodes_match = re.search(r"Nodes.*?│\s+(\d+)", record)
            assert nodes_match, "Could not find Nodes in table"
            nodes_num = int(nodes_match.group(1))
            assert nodes_num > 0, f"Only found {nodes_num} nodes."

            seeds_match = re.search(r"Seeds.*?│\s+(\d+)", record)
            assert seeds_match, "Could not find Seeds in table"
            seeds_num = int(seeds_match.group(1))
            assert seeds_num > 0, f"Only found {seeds_num} seeds."

            sources_match = re.search(r"Sources.*?│\s+(\d+)", record)
            assert sources_match, "Could not find Sources in table"
            sources_num = int(sources_match.group(1))
            assert sources_num > 0, f"Only found {sources_num} sources."

            tests_match = re.search(r"Tests.*?│\s+(\d+)", record)
            assert tests_match, "Could not find Tests in table"
            tests_num = int(tests_match.group(1))
            assert tests_num > 0, f"Only found {tests_num} tests."

    assert summary_count_artifacts == 1
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
        app,
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
        app,
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
        app,
        cli_args.split(" "),
    )
    assert result.exit_code != 0


def test_cli_config_file_doesnt_exist():
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "--config-file",
            "non-existent-file.yml",
        ],
    )
    assert type(result.exception) in [FileNotFoundError, RuntimeError]
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
        app,
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
        app,
        [
            "--config-file",
            Path(tmp_path / "dbt-bouncer-example.yml").__str__(),
        ],
    )
    assert type(result.exception) in [FileNotFoundError]
    assert result.exception.args[0].find("No manifest.json found at") == 0  # type: ignore[union-attr]
    assert result.exit_code != 0


def test_cli_manifest_group_owner_email_list():
    """Test that manifests with group owner email as a list are parsed correctly."""
    import orjson

    from dbt_bouncer.artifact_parsers.parsers_manifest import parse_manifest

    manifest_json_path = Path("dbt_project") / "target/manifest.json"
    manifest = orjson.loads(manifest_json_path.read_bytes())

    manifest["groups"] = {
        "group.dbt_bouncer_test_project.analytics_engineering": {
            "name": "analytics_engineering",
            "resource_type": "group",
            "package_name": "dbt_bouncer_test_project",
            "path": "models/",
            "original_file_path": "models/schema.yml",
            "unique_id": "group.dbt_bouncer_test_project.analytics_engineering",
            "owner": {
                "name": "Analytics Engineering Team",
                "email": [
                    "user1@example.com",
                    "user2@example.com",
                    "user3@example.com",
                ],
            },
        }
    }

    parsed_manifest = parse_manifest(manifest)

    group_email = parsed_manifest.groups[
        "group.dbt_bouncer_test_project.analytics_engineering"
    ].owner.email
    assert group_email == [
        "user1@example.com",
        "user2@example.com",
        "user3@example.com",
    ]


def test_cli_manifest_group_owner_email_string():
    """Test that manifests with group owner email as a string are parsed correctly."""
    import orjson

    from dbt_bouncer.artifact_parsers.parsers_manifest import parse_manifest

    manifest_json_path = Path("dbt_project") / "target/manifest.json"
    manifest = orjson.loads(manifest_json_path.read_bytes())

    manifest["groups"] = {
        "group.dbt_bouncer_test_project.analytics_engineering": {
            "name": "analytics_engineering",
            "resource_type": "group",
            "package_name": "dbt_bouncer_test_project",
            "path": "models/",
            "original_file_path": "models/schema.yml",
            "unique_id": "group.dbt_bouncer_test_project.analytics_engineering",
            "owner": {
                "name": "Analytics Engineering Team",
                "email": "single@example.com",
            },
        }
    }

    parsed_manifest = parse_manifest(manifest)

    group_email = parsed_manifest.groups[
        "group.dbt_bouncer_test_project.analytics_engineering"
    ].owner.email
    assert group_email == "single@example.com"
