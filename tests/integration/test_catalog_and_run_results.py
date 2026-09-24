"""Integration tests for `catalog_checks` and `run_results_checks`, end to end.

The shipped example config runs both categories on the happy path only (see
`test_run_command.py`). These tests cover what that leaves out: a failing check
in either category gating the run, and a configured category whose artifact is
missing from the artifacts directory.
"""

import shutil
from pathlib import PurePath

import pytest

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app
from tests.integration.constants import DBT_112_TARGET, strip_ansi

_ORDERS = r"models/marts/finance/orders\.sql"
_STG_ORDERS = r"models/staging/crm/stg_orders\.sql"

# Per category, a check that passes and one that fails, both scoped to a single
# model so the counts pin down exactly one outcome of each:
# - every `stg_orders` column is lowercase snake_case, and no `orders` column
#   starts with `zzz_`;
# - in the dbt_112 fixture `stg_orders` runs in ~0.07s and `orders` in ~0.10s.
_PASSING_CHECK = {
    "catalog_checks": {
        "name": "check_column_names",
        "include": _STG_ORDERS,
        "column_name_pattern": "[a-z_]+",
    },
    "run_results_checks": {
        "name": "check_run_results_max_execution_time",
        "include": _STG_ORDERS,
        "max_execution_time_seconds": 60,
    },
}
_FAILING_CHECK = {
    "catalog_checks": {
        "name": "check_column_names",
        "include": _ORDERS,
        "column_name_pattern": "zzz_.*",
    },
    "run_results_checks": {
        "name": "check_run_results_max_execution_time",
        "include": _ORDERS,
        "max_execution_time_seconds": 0.001,
    },
}
# The artifact each category needs on top of `manifest.json`.
_ARTIFACT = {
    "catalog_checks": "catalog.json",
    "run_results_checks": "run_results.json",
}

_CATEGORIES = pytest.mark.parametrize(
    "category", ["catalog_checks", "run_results_checks"], ids=["catalog", "run_results"]
)


def _run(cli_runner, config_file):
    """Invoke `dbt-bouncer run` against `config_file`.

    Returns:
        Result: The `CliRunner` result.

    """
    return cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )


@_CATEGORIES
def test_passing_check_exits_success(cli_runner, write_config, category):
    """The passing check alone exits SUCCESS.

    The control for `test_failing_check_exits_check_errors`: it shows the
    failure there comes from the failing check, not from the category itself.
    """
    config_file = write_config(
        {"dbt_artifacts_dir": str(DBT_112_TARGET), category: [_PASSING_CHECK[category]]}
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert "All checks passed! SUCCESS=1 WARN=0 ERROR=0" in strip_ansi(result.output)


@_CATEGORIES
def test_failing_check_exits_check_errors(caplog, cli_runner, write_config, category):
    """A failing check in the category exits CHECK_ERRORS and counts one of each."""
    config_file = write_config(
        {
            "dbt_artifacts_dir": str(DBT_112_TARGET),
            category: [_PASSING_CHECK[category], _FAILING_CHECK[category]],
        }
    )

    result = _run(cli_runner, config_file)

    clean_output = strip_ansi(result.output)
    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    assert "Done. SUCCESS=1 WARN=0 ERROR=1" in clean_output
    assert "Failed checks" in clean_output
    assert "`dbt-bouncer` failed." in caplog.text


@_CATEGORIES
def test_missing_artifact_exits_artifact_error(
    caplog, cli_runner, tmp_path, write_config, category
):
    """A configured category whose artifact is absent exits ARTIFACT_ERROR.

    The directory holds a valid `manifest.json`, so the missing artifact named
    in the error is the only thing that can stop the run.
    """
    artifacts_dir = tmp_path / "target"
    artifacts_dir.mkdir()
    shutil.copy(DBT_112_TARGET / "manifest.json", artifacts_dir / "manifest.json")
    config_file = write_config(
        {"dbt_artifacts_dir": str(artifacts_dir), category: [_PASSING_CHECK[category]]}
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.ARTIFACT_ERROR, result.output
    assert f"No {_ARTIFACT[category]} found at" in caplog.text
