import json
from pathlib import Path, PurePath

import pytest
import yaml
from click.testing import CliRunner

from dbt_bouncer.main import cli
from dbt_bouncer.utils import get_check_objects


def test_cli_coverage_non_json(tmp_path):
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

    # Run dbt-bouncer
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            Path(tmp_path / "dbt-bouncer.yml").__str__(),
            "--output-file",
            tmp_path / "coverage.js",
        ],
    )

    assert isinstance(result.exception, RuntimeError)
    assert (
        result.exception.args[0].find(
            "Output file must have a `.json` extension. Got `.js`.",
        )
        == 0
    )
    assert result.exit_code == 1


# No idea why but this test always fails when run with all other unit tests but succeeds when run alone or when just this file is tested
@pytest.mark.not_in_parallel
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
        == "Staging models should start with 'staging_'. - AssertionError: `stg_orders` does not match the supplied regex `staging_)`."
    )

    assert (
        next(
            x["failure_message"]
            for x in coverage
            if x["check_run_id"] == "check_model_names:1:stg_orders"
        )
        == "AssertionError: `stg_orders` does not match the supplied regex `staging_)`."
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
            9,
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
NUM_RUN_RESULTS_CHECKS = 43


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
