import json
from pathlib import Path

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
    Path(tmp_path / "my_checks_dir/manifest/check_models.py").write_text("""
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
""")

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
        ["--config-file", "dbt-bouncer.yml", "-v"],
    )

    assert len([r for r in caplog.messages if r.find("custom_checks_dir=") >= 0]) >= 1

    assert result.exit_code == 0


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
        assert i not in [
            x["check_run_id"] for x in coverage
        ], f"`{i}` in `coverage.json` when it should be excluded."


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
        assert i not in [
            x["check_run_id"] for x in coverage
        ], f"`{i}` in `coverage.json` when it should be excluded."

    for i in [
        "check_model_access:0:stg_payments",
    ]:
        assert i in [
            x["check_run_id"] for x in coverage
        ], f"`{i}` not in `coverage.json`."


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
        assert i not in [
            x["check_run_id"] for x in coverage
        ], f"`{i}` in `coverage.json` when it should be excluded."


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
        assert i in [
            x["check_run_id"] for x in coverage
        ], f"`{i}` not in `coverage.json`."


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
        assert i not in [
            x["check_run_id"] for x in coverage
        ], f"`{i}` in `coverage.json` when it should be excluded."

    for i in [
        "check_model_access:0:stg_payments",
    ]:
        assert i in [
            x["check_run_id"] for x in coverage
        ], f"`{i}` not in `coverage.json`."


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
        assert i not in [
            x["check_run_id"] for x in coverage
        ], f"`{i}` in `coverage.json` when it should be excluded."

    for i in [
        "check_model_access:0:stg_payments",
    ]:
        assert i in [
            x["check_run_id"] for x in coverage
        ], f"`{i}` not in `coverage.json`."


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
        assert i in [
            x["check_run_id"] for x in coverage
        ], f"`{i}` not in `coverage.json`."


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
        assert i in [
            x["check_run_id"] for x in coverage
        ], f"`{i}` not in `coverage.json`."


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
