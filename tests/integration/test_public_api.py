"""Integration tests for the programmatic entry point, `run_bouncer`."""

from pathlib import Path

import pytest

from dbt_bouncer.cli.run.utils import run_bouncer
from dbt_bouncer.exceptions import DbtBouncerConfigError
from tests.integration.constants import DBT_PROJECT_TARGET, FIXTURES_DIR


def test_programmatic_happy_path(write_checks_config):
    """A passing config returns exit code 0 without going through the CLI."""
    config_file = write_checks_config(
        [
            {
                "name": "check_model_names",
                "include": "^models/staging",
                "model_name_pattern": "^stg_",
            }
        ],
        artifacts_dir=DBT_PROJECT_TARGET,
    )

    assert run_bouncer(config_file=config_file) == 0


# dbt_110 is deliberate: it is a frozen fixture that is no longer rebuilt, so this
# asserts the oldest and newest supported artifact formats both still parse.
@pytest.mark.parametrize("fixture_name", ["dbt_110", "dbt_20"])
def test_programmatic_failure_path(fixture_name, write_checks_config):
    """A failing config returns exit code 1 on both the oldest and newest formats."""
    config_file = write_checks_config(
        [
            {
                "name": "check_model_names",
                "include": "^models/staging",
                "model_name_pattern": "^bad_prefix_",
            }
        ],
        artifacts_dir=FIXTURES_DIR / fixture_name / "target",
    )

    assert run_bouncer(config_file=config_file) == 1


def test_programmatic_missing_config():
    """A missing config file raises rather than returning an exit code."""
    with pytest.raises(DbtBouncerConfigError):
        run_bouncer(config_file=Path("non-existent-config.yml"))
