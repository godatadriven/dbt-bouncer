import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from dbt_bouncer.main import cli


def test_cli_coverage_non_json(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "name": "check_model_directories",
                "include": "",
                "permitted_sub_directories": ["staging"],
            }
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
        result.exception.args[0].find("Output file must have a `.json` extension. Got `.js`.") == 0
    )
    assert result.exit_code == 1


def test_cli_exclude(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "protected",
                "exclude": "^staging",
                "name": "check_model_access",
            }
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

    for i in [
        "check_model_access::stg_customers_0",
        "check_model_access::stg_orders_0",
        "check_model_access::stg_payments_0",
    ]:
        assert i not in [
            x["nodeid"] for x in coverage
        ], f"`{i}` in `coverage.json` when it should be excluded."


def test_cli_exclude_and_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "protected",
                "exclude": "^staging/crm",
                "include": "^staging",
                "name": "check_model_access",
            }
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

    for i in [
        "check_model_access::stg_customers_0",
        "check_model_access::stg_orders_0",
    ]:
        assert i not in [
            x["nodeid"] for x in coverage
        ], f"`{i}` in `coverage.json` when it should be excluded."

    for i in [
        "check_model_access::stg_payments_0",
    ]:
        assert i in [x["nodeid"] for x in coverage], f"`{i}` not in `coverage.json`."


def test_cli_global_exclude(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "exclude": "^staging",
        "manifest_checks": [
            {
                "access": "protected",
                "name": "check_model_access",
            }
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

    for i in [
        "check_model_access::stg_customers_0",
        "check_model_access::stg_orders_0",
        "check_model_access::stg_payments_0",
    ]:
        assert i not in [
            x["nodeid"] for x in coverage
        ], f"`{i}` in `coverage.json` when it should be excluded."


def test_cli_global_and_local_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "include": "^marts",
        "manifest_checks": [
            {
                "access": "protected",
                "include": "^staging",
                "name": "check_model_access",
            }
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

    for i in [
        "check_model_access::stg_customers_0",
        "check_model_access::stg_orders_0",
        "check_model_access::stg_payments_0",
    ]:
        assert i in [x["nodeid"] for x in coverage], f"`{i}` not in `coverage.json`."


def test_cli_global_exclude_and_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "exclude": "^staging/crm",
        "include": "^staging",
        "manifest_checks": [
            {
                "access": "protected",
                "name": "check_model_access",
            }
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

    for i in [
        "check_model_access::stg_customers_0",
        "check_model_access::stg_orders_0",
    ]:
        assert i not in [
            x["nodeid"] for x in coverage
        ], f"`{i}` in `coverage.json` when it should be excluded."

    for i in [
        "check_model_access::stg_payments_0",
    ]:
        assert i in [x["nodeid"] for x in coverage], f"`{i}` not in `coverage.json`."


def test_cli_global_exclude_and_local_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "exclude": "^staging/crm",
        "manifest_checks": [
            {
                "access": "protected",
                "include": "^staging",
                "name": "check_model_access",
            }
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

    for i in [
        "check_model_access::stg_customers_0",
        "check_model_access::stg_orders_0",
    ]:
        assert i not in [
            x["nodeid"] for x in coverage
        ], f"`{i}` in `coverage.json` when it should be excluded."

    for i in [
        "check_model_access::stg_payments_0",
    ]:
        assert i in [x["nodeid"] for x in coverage], f"`{i}` not in `coverage.json`."


def test_cli_global_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "include": "^staging",
        "manifest_checks": [
            {
                "access": "protected",
                "name": "check_model_access",
            }
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

    for i in [
        "check_model_access::stg_customers_0",
        "check_model_access::stg_orders_0",
        "check_model_access::stg_payments_0",
    ]:
        assert i in [x["nodeid"] for x in coverage], f"`{i}` not in `coverage.json`."


def test_cli_include(tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "access": "protected",
                "include": "^staging",
                "name": "check_model_access",
            }
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

    for i in [
        "check_model_access::stg_customers_0",
        "check_model_access::stg_orders_0",
        "check_model_access::stg_payments_0",
    ]:
        assert i in [x["nodeid"] for x in coverage], f"`{i}` not in `coverage.json`."
