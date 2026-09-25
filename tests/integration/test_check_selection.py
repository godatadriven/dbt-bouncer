"""Integration tests for narrowing a run with `--check`, `--only` and `--dry-run`.

The config holds one check per category, each scoped to `stg_orders` so it runs
exactly once and passes. The JSON output file then lists exactly which checks
ran, which is what `--check` and `--only` are meant to change.
"""

import json
import re
from pathlib import PurePath

import pytest

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app
from tests.integration.constants import DBT_112_TARGET, strip_ansi

_STG_ORDERS = r"models/staging/crm/stg_orders\.sql"

_CONFIG = {
    "catalog_checks": [
        {
            "name": "check_column_names",
            "include": _STG_ORDERS,
            "column_name_pattern": "[a-z_]+",
        }
    ],
    "dbt_artifacts_dir": str(DBT_112_TARGET),
    "manifest_checks": [
        {
            "name": "check_model_names",
            "include": _STG_ORDERS,
            "model_name_pattern": "^stg_",
        }
    ],
    "run_results_checks": [
        {
            "name": "check_run_results_max_execution_time",
            "include": _STG_ORDERS,
            "max_execution_time_seconds": 60,
        }
    ],
}

_CATALOG = "check_column_names"
_MANIFEST = "check_model_names"
_RUN_RESULTS = "check_run_results_max_execution_time"


def _checks_run(cli_runner, config_file, tmp_path, *args):
    """Run the CLI with `args` and return the exit code and the check names that ran.

    Returns:
        tuple[int, set[str]]: The exit code and the names in the output file
            (empty if no output file was written).

    """
    output_file = tmp_path / "results.json"
    result = cli_runner.invoke(
        app,
        [
            "run",
            "--config-file",
            PurePath(config_file).as_posix(),
            "--output-file",
            str(output_file),
            *args,
        ],
    )
    if not output_file.exists():
        return result.exit_code, set()
    results = json.loads(output_file.read_text(encoding="utf-8"))
    return result.exit_code, {r["check_run_id"].split(":")[0] for r in results}


def test_no_filter_runs_every_category(cli_runner, tmp_path, write_config):
    """Without a filter, one check from each category runs."""
    exit_code, ran = _checks_run(cli_runner, write_config(_CONFIG), tmp_path)

    assert exit_code == ExitCode.SUCCESS
    assert ran == {_CATALOG, _MANIFEST, _RUN_RESULTS}


@pytest.mark.parametrize(
    ("only", "expected"),
    [
        ("manifest_checks", {_MANIFEST}),
        ("catalog_checks,run_results_checks", {_CATALOG, _RUN_RESULTS}),
        (" catalog_checks , manifest_checks ", {_CATALOG, _MANIFEST}),
    ],
    ids=["one", "two", "whitespace"],
)
def test_only_limits_the_categories_run(
    cli_runner, expected, only, tmp_path, write_config
):
    """`--only` runs just the named categories and skips the rest."""
    exit_code, ran = _checks_run(
        cli_runner, write_config(_CONFIG), tmp_path, "--only", only
    )

    assert exit_code == ExitCode.SUCCESS
    assert ran == expected


def test_only_with_an_invalid_category_is_config_error(
    caplog, cli_runner, tmp_path, write_config
):
    """An unknown `--only` value is a config error that names the valid categories."""
    exit_code, ran = _checks_run(
        cli_runner, write_config(_CONFIG), tmp_path, "--only", "model_checks"
    )

    assert exit_code == ExitCode.CONFIG_ERROR
    assert ran == set()
    assert "`--only` contains an invalid value (`model_checks`)" in caplog.text


def test_check_limits_the_checks_run(cli_runner, tmp_path, write_config):
    """`--check a,b` runs exactly those checks, across categories."""
    exit_code, ran = _checks_run(
        cli_runner,
        write_config(_CONFIG),
        tmp_path,
        "--check",
        f"{_CATALOG},{_RUN_RESULTS}",
    )

    assert exit_code == ExitCode.SUCCESS
    assert ran == {_CATALOG, _RUN_RESULTS}


def test_check_and_only_intersect(cli_runner, tmp_path, write_config):
    """`--check` applies within the categories `--only` kept."""
    exit_code, ran = _checks_run(
        cli_runner,
        write_config(_CONFIG),
        tmp_path,
        "--only",
        "manifest_checks,run_results_checks",
        "--check",
        f"{_CATALOG},{_MANIFEST}",
    )

    assert exit_code == ExitCode.SUCCESS
    assert ran == {_MANIFEST}


def test_check_with_only_an_unknown_name_exits_no_checks_run(
    caplog, cli_runner, tmp_path, write_config
):
    """A `--check` naming nothing in the config warns, runs nothing and exits NO_CHECKS_RUN."""
    exit_code, ran = _checks_run(
        cli_runner,
        write_config(_CONFIG),
        tmp_path,
        "--check",
        "check_model_does_not_exist",
    )

    assert exit_code == ExitCode.NO_CHECKS_RUN
    assert ran == set()
    assert (
        "`--check` contains values not found in the (possibly filtered) config: "
        "['check_model_does_not_exist']"
    ) in caplog.text
    assert "No checks were run." in caplog.text


def test_check_with_a_known_and_an_unknown_name_runs_the_known_one(
    caplog, cli_runner, tmp_path, write_config
):
    """An unknown name alongside a known one warns but still runs the known check."""
    exit_code, ran = _checks_run(
        cli_runner,
        write_config(_CONFIG),
        tmp_path,
        "--check",
        f"{_MANIFEST},check_model_does_not_exist",
    )

    assert exit_code == ExitCode.SUCCESS
    assert ran == {_MANIFEST}
    assert "['check_model_does_not_exist']" in caplog.text


def test_dry_run_lists_checks_without_running_them(cli_runner, tmp_path, write_config):
    """`--dry-run` prints each check's resource type and count, and runs nothing.

    The config fails on every model, so a zero exit code and a missing output
    file both show the checks were not executed.
    """
    config_file = write_config(
        {
            **_CONFIG,
            "manifest_checks": [
                {"name": "check_model_names", "model_name_pattern": "^zzz_"}
            ],
        }
    )
    output_file = tmp_path / "results.json"

    result = cli_runner.invoke(
        app,
        [
            "run",
            "--config-file",
            PurePath(config_file).as_posix(),
            "--dry-run",
            "--output-file",
            str(output_file),
        ],
    )

    output = strip_ansi(result.output)
    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert not output_file.exists()
    assert "Dry run — checks that would execute" in output
    # One row per check: the class name, its resource type and the number of
    # resources it matched (every model in the project, or just `stg_orders`).
    for row in (
        r"CheckColumnNames\s*[│|]\s*catalog_node\s*[│|]\s*1\s",
        r"CheckModelNames\s*[│|]\s*model\s*[│|]\s*14\s",
        r"CheckRunResultsMaxExecutionTime\s*[│|]\s*run_result\s*[│|]\s*1\s",
    ):
        assert re.search(row, output), f"No dry-run row matching {row!r}"
    assert "Dry run complete. 16 check(s) would run." in output
    assert "Failed checks" not in output


def test_dry_run_honours_only(cli_runner, write_config):
    """`--dry-run` plans only the categories `--only` kept."""
    result = cli_runner.invoke(
        app,
        [
            "run",
            "--config-file",
            PurePath(write_config(_CONFIG)).as_posix(),
            "--dry-run",
            "--only",
            "manifest_checks",
        ],
    )

    output = strip_ansi(result.output)
    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert "CheckModelNames" in output
    assert "CheckColumnNames" not in output
    assert "Dry run complete. 1 check(s) would run." in output
