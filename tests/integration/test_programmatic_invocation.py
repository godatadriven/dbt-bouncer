from pathlib import Path

import pytest
import yaml

from dbt_bouncer.main import run_bouncer


def test_programmatic_happy_path(tmp_path):
    artifacts_dir = Path("dbt_project/target").absolute()

    config_data = {
        "dbt_artifacts_dir": str(artifacts_dir),
        "manifest_checks": [
            {
                "name": "check_model_names",
                "include": "^models/staging",
                "model_name_pattern": "^stg_",
            }
        ],
    }
    config_file = tmp_path / "dbt-bouncer.yml"
    with config_file.open("w") as f:
        yaml.dump(config_data, f)

    exit_code = run_bouncer(config_file=config_file)
    assert exit_code == 0


def test_programmatic_failure_path(tmp_path):
    artifacts_dir = Path("tests/fixtures/dbt_17/target").absolute()

    config_data = {
        "dbt_artifacts_dir": str(artifacts_dir),
        "manifest_checks": [
            {
                "name": "check_model_names",
                "include": "^models/staging",
                "model_name_pattern": "^bad_prefix_",
            }
        ],
    }
    config_file = tmp_path / "dbt-bouncer.yml"
    with config_file.open("w") as f:
        yaml.dump(config_data, f)

    exit_code = run_bouncer(config_file=config_file)
    assert exit_code == 1


def test_programmatic_missing_config():
    with pytest.raises(FileNotFoundError or RuntimeError):  # type: ignore[truthy-function]
        run_bouncer(config_file=Path("non-existent-config.yml"))
