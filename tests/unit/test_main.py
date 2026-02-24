import json
from pathlib import Path, PurePath

import pytest
import yaml
from click.testing import CliRunner

from dbt_bouncer.main import cli
from dbt_bouncer.utils import get_check_objects


@pytest.mark.parametrize(
    ("output_format", "output_file_suffix", "is_json"),
    [
        ("csv", "coverage.csv", False),
        ("json", "coverage.json", True),
        ("junit", "coverage.xml", False),
        ("sarif", "coverage.sarif", True),
        ("tap", "coverage.tap", False),
    ],
)
def test_cli_output_formats(output_format, output_file_suffix, is_json, tmp_path):
    """Test that --output-format writes the correct format to --output-file."""
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "name": "check_model_directories",
                "include": "models",
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

    output_file = tmp_path / output_file_suffix

    # Run dbt-bouncer
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            Path(tmp_path / "dbt-bouncer.yml").__str__(),
            "--output-file",
            output_file,
            "--output-format",
            output_format,
        ],
    )

    assert output_file.exists()
    content = output_file.read_text()
    if is_json:
        assert json.loads(output_file.read_bytes())
    elif output_format == "csv":
        assert "check_run_id" in content
    elif output_format == "junit":
        assert "<?xml" in content
        assert "testsuite" in content
    elif output_format == "tap":
        assert content.startswith("TAP version 13")
    assert result.exit_code == 1  # checks fail due to invalid directories


def test_cli_custom_checks_dir(caplog, monkeypatch, tmp_path):
    get_check_objects.cache_clear()

    # Config file
    bouncer_config = {
        "custom_checks_dir": "my_checks_dir",
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "name": "check_my_custom_check",
            },
        ],
    }

    with Path(tmp_path / "dbt-bouncer.yml").open("w") as f:
        yaml.dump(bouncer_config, f)

    Path(tmp_path / "my_checks_dir/manifest").mkdir(parents=True)
    Path(tmp_path / "my_checks_dir/__init__.py").write_text("")
    Path(tmp_path / "my_checks_dir/manifest/check_models.py").write_text(
        """
from pydantic import Field
from typing import Literal
from dbt_bouncer.check_base import BaseCheck
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_bouncer.parsers import DbtBouncerModelBase

class CheckMyCustomCheck(BaseCheck):
    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_my_custom_check"]

    def execute(self) -> None:
        assert 1 == 1
"""
    )

    # Manifest file
    with Path.open(Path("./dbt_project/target/manifest.json"), "r") as f:
        manifest = json.load(f)

    with Path.open(tmp_path / "manifest.json", "w") as f:
        json.dump(manifest, f)

    # Run dbt-bouncer
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--config-file", PurePath("dbt-bouncer.yml").as_posix()],
    )

    assert len([r for r in caplog.messages if r.find("custom_checks_dir=") >= 0]) >= 1

    assert result.exit_code == 0


def test_cli_description(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "description": "Staging models should start with 'staging_'.",
                "include": "^models/staging",
                "model_name_pattern": "staging_",
                "name": "check_model_names",
            },
            {
                "include": "^models/staging",
                "model_name_pattern": "staging_",
                "name": "check_model_names",
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
    runner.invoke(
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

    import logging

    logging.warning(f"{coverage=}")

    assert (
        next(
            x["failure_message"]
            for x in coverage
            if x["check_run_id"] == "check_model_names:0:stg_orders"
        )
        == "Staging models should start with 'staging_'. - `stg_orders` does not match the supplied regex `staging_`."
    )

    assert (
        next(
            x["failure_message"]
            for x in coverage
            if x["check_run_id"] == "check_model_names:1:stg_orders"
        )
        == "`stg_orders` does not match the supplied regex `staging_`."
    )


def test_cli_exclude(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "protected",
                "exclude": "^models/staging",
                "name": "check_model_access",
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
    runner.invoke(
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

    for i in [
        "check_model_access:0:stg_customers",
        "check_model_access:0:stg_orders",
        "check_model_access:0:stg_payments",
    ]:
        assert i not in [x["check_run_id"] for x in coverage], (
            f"`{i}` in `coverage.json` when it should be excluded."
        )


def test_cli_exclude_and_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "protected",
                "exclude": "^models/staging/crm",
                "include": "^models/staging",
                "name": "check_model_access",
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
    runner.invoke(
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

    for i in [
        "check_model_access:0:stg_customers",
        "check_model_access:0:stg_orders",
    ]:
        assert i not in [x["check_run_id"] for x in coverage], (
            f"`{i}` in `coverage.json` when it should be excluded."
        )

    for i in [
        "check_model_access:0:stg_payments",
    ]:
        assert i in [x["check_run_id"] for x in coverage], (
            f"`{i}` not in `coverage.json`."
        )


def test_cli_global_exclude(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "exclude": "^models/staging",
        "manifest_checks": [
            {
                "access": "protected",
                "name": "check_model_access",
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
    runner.invoke(
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

    for i in [
        "check_model_access:0:stg_customers",
        "check_model_access:0:stg_orders",
        "check_model_access:0:stg_payments",
    ]:
        assert i not in [x["check_run_id"] for x in coverage], (
            f"`{i}` in `coverage.json` when it should be excluded."
        )


def test_cli_global_and_local_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "include": "^models/marts",
        "manifest_checks": [
            {
                "access": "protected",
                "include": "^models/staging",
                "name": "check_model_access",
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
    runner.invoke(
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

    for i in [
        "check_model_access:0:stg_customers",
        "check_model_access:0:stg_orders",
        "check_model_access:0:stg_payments",
    ]:
        assert i in [x["check_run_id"] for x in coverage], (
            f"`{i}` not in `coverage.json`."
        )


def test_cli_global_exclude_and_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "exclude": "^models/staging/crm",
        "include": "^models/staging",
        "manifest_checks": [
            {
                "access": "protected",
                "name": "check_model_access",
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
    runner.invoke(
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

    for i in [
        "check_model_access:0:stg_customers",
        "check_model_access:0:stg_orders",
    ]:
        assert i not in [x["check_run_id"] for x in coverage], (
            f"`{i}` in `coverage.json` when it should be excluded."
        )

    for i in [
        "check_model_access:0:stg_payments",
    ]:
        assert i in [x["check_run_id"] for x in coverage], (
            f"`{i}` not in `coverage.json`."
        )


def test_cli_global_exclude_and_local_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "exclude": "^models/staging/crm",
        "manifest_checks": [
            {
                "access": "protected",
                "include": "^models/staging",
                "name": "check_model_access",
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
    runner.invoke(
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

    for i in [
        "check_model_access:0:stg_customers",
        "check_model_access:0:stg_orders",
    ]:
        assert i not in [x["check_run_id"] for x in coverage], (
            f"`{i}` in `coverage.json` when it should be excluded."
        )

    for i in [
        "check_model_access:0:stg_payments",
    ]:
        assert i in [x["check_run_id"] for x in coverage], (
            f"`{i}` not in `coverage.json`."
        )


def test_cli_global_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "include": "^models/staging",
        "manifest_checks": [
            {
                "access": "protected",
                "name": "check_model_access",
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
    runner.invoke(
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

    for i in [
        "check_model_access:0:stg_customers",
        "check_model_access:0:stg_orders",
        "check_model_access:0:stg_payments",
    ]:
        assert i in [x["check_run_id"] for x in coverage], (
            f"`{i}` not in `coverage.json`."
        )


def test_cli_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "protected",
                "include": "^models/staging",
                "name": "check_model_access",
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
    runner.invoke(
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

    for i in [
        "check_model_access:0:stg_customers",
        "check_model_access:0:stg_orders",
        "check_model_access:0:stg_payments",
    ]:
        assert i in [x["check_run_id"] for x in coverage], (
            f"`{i}` not in `coverage.json`."
        )


@pytest.mark.parametrize(
    ("manifest_check", "num_checks"),
    [
        (
            {
                "access": "protected",
                "name": "check_model_access",
            },
            12,
        ),
        (
            {
                "access": "protected",
                "materialization": "view",
                "name": "check_model_access",
            },
            4,
        ),
        (
            {
                "access": "protected",
                "materialization": "ephemeral",
                "name": "check_model_access",
            },
            2,
        ),
        (
            {
                "access": "protected",
                "include": "^models/intermediate",
                "materialization": "ephemeral",
                "name": "check_model_access",
            },
            2,
        ),
        (
            {
                "access": "protected",
                "include": "^models/intermediate",
                "materialization": "table",
                "name": "check_model_access",
            },
            0,
        ),
    ],
)
def test_cli_materialization(manifest_check, num_checks, tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            manifest_check,
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
    runner.invoke(
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

    assert len(coverage) == num_checks


NUM_CATALOG_CHECKS = 1
NUM_MANIFEST_CHECKS = 3
NUM_RUN_RESULTS_CHECKS = 51


@pytest.mark.parametrize(
    ("only_value", "exit_code", "number_of_checks_run"),
    [
        ("", 0, NUM_CATALOG_CHECKS + NUM_MANIFEST_CHECKS + NUM_RUN_RESULTS_CHECKS),
        ("catalog_checks", 0, NUM_CATALOG_CHECKS),
        ("catalog_checks,manifest_checks", 0, NUM_CATALOG_CHECKS + NUM_MANIFEST_CHECKS),
        (
            "catalog_checks,manifest_checks,run_results_checks",
            0,
            NUM_CATALOG_CHECKS + NUM_MANIFEST_CHECKS + NUM_RUN_RESULTS_CHECKS,
        ),
        (
            "catalog_checks, run_results_checks",
            0,
            NUM_CATALOG_CHECKS + NUM_RUN_RESULTS_CHECKS,
        ),
        ("manifest_checks", 0, NUM_MANIFEST_CHECKS),
        ("run_results_checks", 0, NUM_RUN_RESULTS_CHECKS),
        ("manifest", 1, 1),
        ("manifest checks", 1, 1),
    ],
)
def test_cli_only(only_value, exit_code, number_of_checks_run, tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "catalog_checks": [
            {"include": "^models/marts", "name": "check_column_description_populated"}
        ],
        "manifest_checks": [
            {
                "access": "protected",
                "include": "^models/staging",
                "name": "check_model_access",
            },
        ],
        "run_results_checks": [
            {
                "max_execution_time_seconds": 10,
                "name": "check_run_results_max_execution_time",
            }
        ],
    }

    with Path(tmp_path / "dbt-bouncer.yml").open("w") as f:
        yaml.dump(bouncer_config, f)

    # Artifact files
    for file in ["catalog.json", "manifest.json", "run_results.json"]:
        with Path.open(Path(f"./dbt_project/target/{file}"), "r") as f:
            data = json.load(f)
        with Path.open(tmp_path / file, "w") as f:
            json.dump(data, f)

    # Run dbt-bouncer
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            Path(tmp_path / "dbt-bouncer.yml").__str__(),
            "--output-file",
            Path(tmp_path / "results.json").__str__(),
            "--only",
            only_value,
        ],
    )
    assert result.exit_code == exit_code

    if exit_code == 0:
        with Path.open(tmp_path / "results.json", "r") as f:
            results = json.load(f)
        assert len(results) == number_of_checks_run


@pytest.mark.parametrize(
    ("check_value", "exit_code", "number_of_checks_run"),
    [
        ("", 0, NUM_CATALOG_CHECKS + NUM_MANIFEST_CHECKS + NUM_RUN_RESULTS_CHECKS),
        ("check_column_description_populated", 0, NUM_CATALOG_CHECKS),
        (
            "check_column_description_populated,check_model_access",
            0,
            NUM_CATALOG_CHECKS + NUM_MANIFEST_CHECKS,
        ),
        ("check_model_access", 0, NUM_MANIFEST_CHECKS),
        ("check_run_results_max_execution_time", 0, NUM_RUN_RESULTS_CHECKS),
    ],
)
def test_cli_check(check_value, exit_code, number_of_checks_run, tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "catalog_checks": [
            {"include": "^models/marts", "name": "check_column_description_populated"}
        ],
        "manifest_checks": [
            {
                "access": "protected",
                "include": "^models/staging",
                "name": "check_model_access",
            },
        ],
        "run_results_checks": [
            {
                "max_execution_time_seconds": 10,
                "name": "check_run_results_max_execution_time",
            }
        ],
    }

    with Path(tmp_path / "dbt-bouncer.yml").open("w") as f:
        yaml.dump(bouncer_config, f)

    # Artifact files
    for file in ["catalog.json", "manifest.json", "run_results.json"]:
        with Path.open(Path(f"./dbt_project/target/{file}"), "r") as f:
            data = json.load(f)
        with Path.open(tmp_path / file, "w") as f:
            json.dump(data, f)

    # Run dbt-bouncer
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            Path(tmp_path / "dbt-bouncer.yml").__str__(),
            "--output-file",
            Path(tmp_path / "results.json").__str__(),
            "--check",
            check_value,
        ],
    )
    assert result.exit_code == exit_code

    with Path.open(tmp_path / "results.json", "r") as f:
        results = json.load(f)
    assert len(results) == number_of_checks_run


@pytest.mark.parametrize(
    ("check_value", "only_value", "exit_code", "number_of_checks_run"),
    [
        # --check filters within the --only category
        ("check_model_access", "manifest_checks", 0, NUM_MANIFEST_CHECKS),
        # --check name not in the --only category: zero checks run
        ("check_run_results_max_execution_time", "manifest_checks", 0, 0),
        # Unknown check name: zero checks run (with warning)
        ("nonexistent_check", "", 0, 0),
    ],
)
def test_cli_check_combined_with_only(
    check_value, only_value, exit_code, number_of_checks_run, tmp_path
):
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "catalog_checks": [
            {"include": "^models/marts", "name": "check_column_description_populated"}
        ],
        "manifest_checks": [
            {
                "access": "protected",
                "include": "^models/staging",
                "name": "check_model_access",
            },
        ],
        "run_results_checks": [
            {
                "max_execution_time_seconds": 10,
                "name": "check_run_results_max_execution_time",
            }
        ],
    }

    with Path(tmp_path / "dbt-bouncer.yml").open("w") as f:
        yaml.dump(bouncer_config, f)

    for file in ["catalog.json", "manifest.json", "run_results.json"]:
        with Path.open(Path(f"./dbt_project/target/{file}"), "r") as f:
            data = json.load(f)
        with Path.open(tmp_path / file, "w") as f:
            json.dump(data, f)

    args = [
        "--config-file",
        Path(tmp_path / "dbt-bouncer.yml").__str__(),
        "--output-file",
        Path(tmp_path / "results.json").__str__(),
        "--check",
        check_value,
    ]
    if only_value:
        args.extend(["--only", only_value])

    runner = CliRunner()
    result = runner.invoke(cli, args)
    assert result.exit_code == exit_code

    with Path.open(tmp_path / "results.json", "r") as f:
        results = json.load(f)
    assert len(results) == number_of_checks_run


@pytest.mark.parametrize(
    ("package_name", "number_of_checks_run"),
    [
        ("dbt_bouncer_test_project", 2),
        ("my_internal_package", 1),
    ],
)
def test_cli_package_name(package_name, number_of_checks_run, tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "protected",
                "name": "check_model_access",
            },
        ],
        "package_name": package_name,
    }

    with Path(tmp_path / "dbt-bouncer.yml").open("w") as f:
        yaml.dump(bouncer_config, f)

    # Artifact files
    manifest_data = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "1.10.15",
            "generated_at": "2025-11-29T21:35:56.918761Z",
            "invocation_id": "c537e3b2-9db0-4aff-8079-4a2f30152780",
            "invocation_started_at": "2025-11-29T21:35:44.640528+00:00",
            "env": {},
            "project_name": "dbt_bouncer_test_project",
            "project_id": "52e2b356b2b0bade408c5d8ef6aa0066",
            "user_id": "96d6c6b1-cd5d-4c8b-b700-2b64b0a5188d",
            "send_anonymous_usage_stats": True,
            "adapter_type": "duckdb",
            "quoting": {
                "database": True,
                "schema": True,
                "identifier": True,
                "column": None,
            },
        },
        "nodes": {
            "model.dbt_bouncer_test_project.stg_orders": {
                "database": "dbt",
                "schema": "main_stg_crm",
                "name": "stg_orders",
                "resource_type": "model",
                "package_name": "dbt_bouncer_test_project",
                "path": "staging/crm/stg_orders.sql",
                "original_file_path": "models/staging/crm/stg_orders.sql",
                "unique_id": "model.dbt_bouncer_test_project.stg_orders",
                "fqn": ["dbt_bouncer_test_project", "staging", "crm", "stg_orders"],
                "alias": "stg_orders",
                "checksum": {
                    "name": "sha256",
                    "checksum": "31f8baee5c6a9ff489abc8670cc62c71dea9c7a080f500ffa9f980e11be1d55c",
                },
                "config": {
                    "enabled": True,
                    "alias": None,
                    "schema": "stg_crm",
                    "database": None,
                    "tags": ["crm"],
                    "meta": {"maturity": "gold"},
                    "group": None,
                    "materialized": "view",
                    "incremental_strategy": None,
                    "batch_size": None,
                    "lookback": 1,
                    "begin": None,
                    "persist_docs": {},
                    "post-hook": [],
                    "pre-hook": [],
                    "quoting": {},
                    "column_types": {},
                    "full_refresh": None,
                    "unique_key": None,
                    "on_schema_change": "ignore",
                    "on_configuration_change": "apply",
                    "grants": {},
                    "packages": [],
                    "docs": {"show": True, "node_color": None},
                    "contract": {"enforced": False, "alias_types": True},
                    "event_time": None,
                    "concurrent_batches": None,
                    "access": "protected",
                    "freshness": None,
                    "internal_package": None,
                },
                "tags": ["crm"],
                "description": "Staging table for orders data.",
                "columns": {
                    "order_id": {
                        "name": "order_id",
                        "description": "",
                        "meta": {},
                        "data_type": None,
                        "constraints": [],
                        "quote": None,
                        "config": {"meta": {}, "tags": []},
                        "tags": [],
                        "granularity": None,
                        "doc_blocks": [],
                    },
                    "status": {
                        "name": "status",
                        "description": "",
                        "meta": {},
                        "data_type": None,
                        "constraints": [],
                        "quote": None,
                        "config": {"meta": {}, "tags": []},
                        "tags": [],
                        "granularity": None,
                        "doc_blocks": [],
                    },
                },
                "meta": {"maturity": "gold"},
                "group": None,
                "docs": {"show": True, "node_color": None},
                "patch_path": "dbt_bouncer_test_project://models/staging/crm/_stg_crm__models.yml",
                "build_path": None,
                "unrendered_config": {
                    "internal_package": None,
                    "access": "protected",
                    "schema": "stg_crm",
                    "tags": ["crm"],
                    "meta": {"maturity": "gold"},
                },
                "created_at": 1764452147.974442,
                "relation_name": '"dbt"."main_stg_crm"."stg_orders"',
                "raw_code": 'with\n    source as (\n\n        {#-\n    Normally we would select from the table here, but we are using seeds to load\n    our data in this project\n    #}\n        select * from {{ ref("raw_orders") }}\n\n    ),\n\n    renamed as (\n\n        select id as order_id, user_id as customer_id, order_date, status from source\n\n    )\n\nselect *\nfrom renamed',
                "doc_blocks": [],
                "language": "sql",
                "refs": [{"name": "raw_orders", "package": None, "version": None}],
                "sources": [],
                "metrics": [],
                "depends_on": {
                    "macros": [],
                    "nodes": ["seed.dbt_bouncer_test_project.raw_orders"],
                },
                "compiled_path": "target/compiled/dbt_bouncer_test_project/models/staging/crm/stg_orders.sql",
                "compiled": True,
                "compiled_code": 'with\n    source as (\n        select * from "dbt"."main"."raw_orders"\n\n    ),\n\n    renamed as (\n\n        select id as order_id, user_id as customer_id, order_date, status from source\n\n    )\n\nselect *\nfrom renamed',
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "contract": {"enforced": False, "alias_types": True, "checksum": None},
                "access": "protected",
                "constraints": [],
                "version": None,
                "latest_version": None,
                "deprecation_date": None,
                "primary_key": ["order_id"],
                "time_spine": None,
            },
            "model.dbt_bouncer_test_project.stg_payments": {
                "database": "dbt",
                "schema": "main_stg_crm",
                "name": "stg_payments",
                "resource_type": "model",
                "package_name": "dbt_bouncer_test_project",
                "path": "staging/crm/stg_payments.sql",
                "original_file_path": "models/staging/crm/stg_payments.sql",
                "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                "fqn": ["dbt_bouncer_test_project", "staging", "crm", "stg_payments"],
                "alias": "stg_payments",
                "checksum": {
                    "name": "sha256",
                    "checksum": "31f8baee5c6a9ff489abc8670cc62c71dea9c7a080f500ffa9f980e11be1d55c",
                },
                "config": {
                    "enabled": True,
                    "alias": None,
                    "schema": "stg_crm",
                    "database": None,
                    "tags": ["crm"],
                    "meta": {"maturity": "gold"},
                    "group": None,
                    "materialized": "view",
                    "incremental_strategy": None,
                    "batch_size": None,
                    "lookback": 1,
                    "begin": None,
                    "persist_docs": {},
                    "post-hook": [],
                    "pre-hook": [],
                    "quoting": {},
                    "column_types": {},
                    "full_refresh": None,
                    "unique_key": None,
                    "on_schema_change": "ignore",
                    "on_configuration_change": "apply",
                    "grants": {},
                    "packages": [],
                    "docs": {"show": True, "node_color": None},
                    "contract": {"enforced": False, "alias_types": True},
                    "event_time": None,
                    "concurrent_batches": None,
                    "access": "protected",
                    "freshness": None,
                    "internal_package": None,
                },
                "tags": ["crm"],
                "description": "Staging table for orders data.",
                "columns": {
                    "order_id": {
                        "name": "order_id",
                        "description": "",
                        "meta": {},
                        "data_type": None,
                        "constraints": [],
                        "quote": None,
                        "config": {"meta": {}, "tags": []},
                        "tags": [],
                        "granularity": None,
                        "doc_blocks": [],
                    },
                    "status": {
                        "name": "status",
                        "description": "",
                        "meta": {},
                        "data_type": None,
                        "constraints": [],
                        "quote": None,
                        "config": {"meta": {}, "tags": []},
                        "tags": [],
                        "granularity": None,
                        "doc_blocks": [],
                    },
                },
                "meta": {"maturity": "gold"},
                "group": None,
                "docs": {"show": True, "node_color": None},
                "patch_path": "dbt_bouncer_test_project://models/staging/crm/_stg_crm__models.yml",
                "build_path": None,
                "unrendered_config": {
                    "internal_package": None,
                    "access": "protected",
                    "schema": "stg_crm",
                    "tags": ["crm"],
                    "meta": {"maturity": "gold"},
                },
                "created_at": 1764452147.974442,
                "relation_name": '"dbt"."main_stg_crm"."stg_orders"',
                "raw_code": 'with\n    source as (\n\n        {#-\n    Normally we would select from the table here, but we are using seeds to load\n    our data in this project\n    #}\n        select * from {{ ref("raw_orders") }}\n\n    ),\n\n    renamed as (\n\n        select id as order_id, user_id as customer_id, order_date, status from source\n\n    )\n\nselect *\nfrom renamed',
                "doc_blocks": [],
                "language": "sql",
                "refs": [{"name": "raw_orders", "package": None, "version": None}],
                "sources": [],
                "metrics": [],
                "depends_on": {
                    "macros": [],
                    "nodes": ["seed.dbt_bouncer_test_project.raw_orders"],
                },
                "compiled_path": "target/compiled/dbt_bouncer_test_project/models/staging/crm/stg_payments.sql",
                "compiled": True,
                "compiled_code": 'with\n    source as (\n        select * from "dbt"."main"."raw_orders"\n\n    ),\n\n    renamed as (\n\n        select id as order_id, user_id as customer_id, order_date, status from source\n\n    )\n\nselect *\nfrom renamed',
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "contract": {"enforced": False, "alias_types": True, "checksum": None},
                "access": "protected",
                "constraints": [],
                "version": None,
                "latest_version": None,
                "deprecation_date": None,
                "primary_key": ["order_id"],
                "time_spine": None,
            },
            "model.my_internal_package.model_1": {
                "database": "dbt",
                "schema": "main_stg_crm",
                "name": "model-1",
                "resource_type": "model",
                "package_name": "my_internal_package",
                "path": "model_1.sql",
                "original_file_path": "models/model_.sql",
                "unique_id": "model.my_internal_package.model_1",
                "fqn": ["my_internal_package", "staging", "crm", "model_1"],
                "alias": "model_1",
                "checksum": {
                    "name": "sha256",
                    "checksum": "31f8baee5c6a9ff489abc8670cc62c71dea9c7a080f500ffa9f980e11be1d55c",
                },
                "config": {
                    "enabled": True,
                    "alias": None,
                    "schema": "stg_crm",
                    "database": None,
                    "tags": ["crm"],
                    "meta": {"maturity": "gold"},
                    "group": None,
                    "materialized": "view",
                    "incremental_strategy": None,
                    "batch_size": None,
                    "lookback": 1,
                    "begin": None,
                    "persist_docs": {},
                    "post-hook": [],
                    "pre-hook": [],
                    "quoting": {},
                    "column_types": {},
                    "full_refresh": None,
                    "unique_key": None,
                    "on_schema_change": "ignore",
                    "on_configuration_change": "apply",
                    "grants": {},
                    "packages": [],
                    "docs": {"show": True, "node_color": None},
                    "contract": {"enforced": False, "alias_types": True},
                    "event_time": None,
                    "concurrent_batches": None,
                    "access": "protected",
                    "freshness": None,
                    "internal_package": None,
                },
                "tags": ["crm"],
                "description": "Staging table for orders data.",
                "columns": {
                    "order_id": {
                        "name": "order_id",
                        "description": "",
                        "meta": {},
                        "data_type": None,
                        "constraints": [],
                        "quote": None,
                        "config": {"meta": {}, "tags": []},
                        "tags": [],
                        "granularity": None,
                        "doc_blocks": [],
                    },
                    "status": {
                        "name": "status",
                        "description": "",
                        "meta": {},
                        "data_type": None,
                        "constraints": [],
                        "quote": None,
                        "config": {"meta": {}, "tags": []},
                        "tags": [],
                        "granularity": None,
                        "doc_blocks": [],
                    },
                },
                "meta": {"maturity": "gold"},
                "group": None,
                "docs": {"show": True, "node_color": None},
                "patch_path": "my_internal_package://models/staging/crm/_stg_crm__models.yml",
                "build_path": None,
                "unrendered_config": {
                    "internal_package": None,
                    "access": "protected",
                    "schema": "stg_crm",
                    "tags": ["crm"],
                    "meta": {"maturity": "gold"},
                },
                "created_at": 1764452147.974442,
                "relation_name": '"dbt"."main_stg_crm"."stg_orders"',
                "raw_code": 'with\n    source as (\n\n        {#-\n    Normally we would select from the table here, but we are using seeds to load\n    our data in this project\n    #}\n        select * from {{ ref("raw_orders") }}\n\n    ),\n\n    renamed as (\n\n        select id as order_id, user_id as customer_id, order_date, status from source\n\n    )\n\nselect *\nfrom renamed',
                "doc_blocks": [],
                "language": "sql",
                "refs": [{"name": "raw_orders", "package": None, "version": None}],
                "sources": [],
                "metrics": [],
                "depends_on": {
                    "macros": [],
                    "nodes": ["seed.my_internal_package.raw_orders"],
                },
                "compiled_path": "target/compiled/my_internal_package/models/staging/crm/model_1.sql",
                "compiled": True,
                "compiled_code": 'with\n    source as (\n        select * from "dbt"."main"."raw_orders"\n\n    ),\n\n    renamed as (\n\n        select id as order_id, user_id as customer_id, order_date, status from source\n\n    )\n\nselect *\nfrom renamed',
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "contract": {"enforced": False, "alias_types": True, "checksum": None},
                "access": "protected",
                "constraints": [],
                "version": None,
                "latest_version": None,
                "deprecation_date": None,
                "primary_key": ["order_id"],
                "time_spine": None,
            },
        },
        "sources": {},
        "macros": {},
        "docs": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "disabled": {},
        "parent_map": {},
        "child_map": {},
        "group_map": {},
        "saved_queries": {},
        "semantic_models": {},
        "unit_tests": {},
    }
    with Path.open(tmp_path / "manifest.json", "w") as f:
        json.dump(manifest_data, f)

    # Run dbt-bouncer
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            Path(tmp_path / "dbt-bouncer.yml").__str__(),
            "--output-file",
            Path(tmp_path / "results.json").__str__(),
        ],
    )
    assert result.exit_code == 0

    if result.exit_code == 0:
        with Path.open(tmp_path / "results.json", "r") as f:
            results = json.load(f)
        assert len(results) == number_of_checks_run


def test_cli_severity_default(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "public",
                "include": "^models/staging",
                "name": "check_model_access",
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

    assert (
        next(
            c
            for c in coverage
            if c["check_run_id"] == "check_model_access:0:stg_customers"
        )["severity"]
        == "error"
    )
    assert result.exit_code == 1


def test_cli_severity_error(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "public",
                "include": "^models/staging",
                "name": "check_model_access",
                "severity": "error",
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

    assert (
        next(
            c
            for c in coverage
            if c["check_run_id"] == "check_model_access:0:stg_customers"
        )["severity"]
        == "error"
    )
    assert result.exit_code == 1


def test_cli_severity_global_priority(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "public",
                "include": "^models/staging",
                "name": "check_model_access",
                "severity": "error",
            }
        ],
        "severity": "warn",
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

    assert (
        next(
            c
            for c in coverage
            if c["check_run_id"] == "check_model_access:0:stg_customers"
        )["severity"]
        == "warn"
    )
    assert result.exit_code == 0


def test_cli_severity_global_warn(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "public",
                "include": "^models/staging",
                "name": "check_model_access",
            }
        ],
        "severity": "warn",
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

    assert (
        next(
            c
            for c in coverage
            if c["check_run_id"] == "check_model_access:0:stg_customers"
        )["severity"]
        == "warn"
    )
    assert result.exit_code == 0


def test_cli_severity_warn(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "public",
                "include": "^models/staging",
                "name": "check_model_access",
                "severity": "warn",
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

    assert (
        next(
            c
            for c in coverage
            if c["check_run_id"] == "check_model_access:0:stg_customers"
        )["severity"]
        == "warn"
    )
    assert result.exit_code == 0


def test_cli_unsupported_dbt_version(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "name": "check_model_directories",
                "include": "models",
                "permitted_sub_directories": ["staging"],
            },
        ],
    }

    with Path(tmp_path / "dbt-bouncer.yml").open("w") as f:
        yaml.dump(bouncer_config, f)

    # Manifest file
    with Path.open(Path("./dbt_project/target/manifest.json"), "r") as f:
        manifest = json.load(f)

    manifest["metadata"]["dbt_version"] = "1.5.5"
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

    assert isinstance(result.exception, AssertionError)
    assert (
        result.exception.args[0].find(
            "The supplied `manifest.json` was generated with dbt version 1.5.5, this is below the minimum supported version of",
        )
        >= 0
    )
    assert result.exit_code == 1


def test_cli_list():
    """Test that `dbt-bouncer list` outputs all built-in checks grouped by category."""
    runner = CliRunner()
    result = runner.invoke(cli, ["list"])

    assert result.exit_code == 0
    # Category headers are present
    assert "catalog_checks:" in result.output
    assert "manifest_checks:" in result.output
    assert "run_results_checks:" in result.output
    # Spot-check a check from each category
    assert "CheckColumnDescriptionPopulated:" in result.output
    assert "CheckModelDescriptionPopulated:" in result.output
    assert "CheckSourceDescriptionPopulated:" in result.output
    assert "CheckRunResultsMaxExecutionTime:" in result.output
    # Descriptions are included
    assert "Columns must have a populated description." in result.output
    # catalog_checks header appears before manifest_checks header
    assert result.output.index("catalog_checks:") < result.output.index(
        "manifest_checks:"
    )
    assert result.output.index("manifest_checks:") < result.output.index(
        "run_results_checks:"
    )


def test_cli_run_command(tmp_path):
    """Test that `dbt-bouncer run` works the same as `dbt-bouncer`."""
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "name": "check_model_directories",
                "include": "models",
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

    output_file = tmp_path / "output.json"

    # Run dbt-bouncer with explicit "run" command
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            "--config-file",
            Path(tmp_path / "dbt-bouncer.yml").__str__(),
            "--output-file",
            output_file,
            "--output-format",
            "json",
        ],
    )

    assert result.exit_code == 1  # Should fail due to directory check
    assert output_file.exists()
    content = json.loads(output_file.read_bytes())
    assert len(content) > 0

    # Verify backwards compatibility: run without "run" command
    output_file_legacy = tmp_path / "output_legacy.json"
    result_legacy = runner.invoke(
        cli,
        [
            "--config-file",
            Path(tmp_path / "dbt-bouncer.yml").__str__(),
            "--output-file",
            output_file_legacy,
            "--output-format",
            "json",
        ],
    )

    assert result_legacy.exit_code == 1  # Should fail the same way
    assert output_file_legacy.exists()
    content_legacy = json.loads(output_file_legacy.read_bytes())
    # Both should produce the same checks
    assert len(content) == len(content_legacy)
