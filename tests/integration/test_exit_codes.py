"""Integration tests for the process exit code the CLI returns.

Every member of `ExitCode` is exercised here, since the exit code is the part of
dbt-bouncer that CI pipelines actually branch on: a regression that turns a
failing check into a zero exit silently stops gating merges.
"""

from pathlib import PurePath

import pytest

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app
from tests.integration.constants import DBT_112_TARGET, strip_ansi

# `orders` does not match `^stg_`, so this check always fails.
_FAILING_CHECK = {
    "name": "check_model_names",
    "include": r"models/marts/finance/orders\.sql",
    "model_name_pattern": "^stg_",
}

# `stg_orders` does match `^stg_`, so this check always passes.
_PASSING_CHECK = {
    "name": "check_model_names",
    "include": r"models/staging/crm/stg_orders\.sql",
    "model_name_pattern": "^stg_",
}

# A second failing check, so a mixed run can hold one of each outcome.
_SECOND_FAILING_CHECK = {
    "name": "check_model_names",
    "include": r"models/marts/finance/customers_v1\.sql",
    "model_name_pattern": "^stg_",
}

_FAILURE_BANNER = "`dbt-bouncer` failed."
_WARNING_BANNER = "`dbt-bouncer` has warnings."


def test_all_checks_passing_exits_success(cli_runner, write_checks_config):
    """A config whose checks all pass exits with SUCCESS."""
    config_file = write_checks_config([_PASSING_CHECK])

    result = cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )

    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert "All checks passed! SUCCESS=1 WARN=0 ERROR=0" in strip_ansi(result.output)


@pytest.mark.parametrize(
    (
        "severity",
        "expected_exit_code",
        "expected_summary",
        "expected_log",
        "unexpected_log",
        "expected_table_title",
        "unexpected_table_title",
    ),
    [
        (
            "warn",
            ExitCode.SUCCESS,
            "Done. SUCCESS=0 WARN=1 ERROR=0",
            _WARNING_BANNER,
            _FAILURE_BANNER,
            "Warning checks",
            "Failed checks",
        ),
        (
            "error",
            ExitCode.CHECK_ERRORS,
            "Done. SUCCESS=0 WARN=0 ERROR=1",
            _FAILURE_BANNER,
            _WARNING_BANNER,
            "Failed checks",
            "Warning checks",
        ),
    ],
    ids=["warn", "error"],
)
def test_check_severity_drives_exit_code_and_summary(
    caplog,
    cli_runner,
    write_checks_config,
    severity,
    expected_exit_code,
    expected_summary,
    expected_log,
    unexpected_log,
    expected_table_title,
    unexpected_table_title,
):
    """A failing check exits SUCCESS at `warn` severity and CHECK_ERRORS at `error`."""
    config_file = write_checks_config([{**_FAILING_CHECK, "severity": severity}])

    result = cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )

    clean_output = strip_ansi(result.output)
    assert result.exit_code == expected_exit_code, result.output
    assert expected_summary in clean_output
    assert expected_log in caplog.text
    assert unexpected_log not in caplog.text
    assert expected_table_title in clean_output
    assert unexpected_table_title not in clean_output


def test_mixed_warn_and_error_exits_check_errors(
    caplog, cli_runner, write_checks_config
):
    """A run holding a pass, a warning and an error reports all three and exits CHECK_ERRORS."""
    config_file = write_checks_config(
        [
            _PASSING_CHECK,
            {**_FAILING_CHECK, "severity": "warn"},
            {**_SECOND_FAILING_CHECK, "severity": "error"},
        ]
    )

    result = cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    assert "Done. SUCCESS=1 WARN=1 ERROR=1" in strip_ansi(result.output)
    assert _FAILURE_BANNER in caplog.text


def test_global_severity_warn_exits_success(caplog, cli_runner, write_config):
    """A top-level `severity: warn` demotes every failing check, so the run exits SUCCESS."""
    config_file = write_config(
        {
            "dbt_artifacts_dir": str(DBT_112_TARGET),
            "manifest_checks": [_FAILING_CHECK],
            "severity": "warn",
        }
    )

    result = cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )

    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert "Done. SUCCESS=0 WARN=1 ERROR=0" in strip_ansi(result.output)
    assert _WARNING_BANNER in caplog.text
    assert _FAILURE_BANNER not in caplog.text


def test_config_matching_no_resources_exits_no_checks_run(
    caplog, cli_runner, write_checks_config
):
    """A config whose `include` matches no model exits NO_CHECKS_RUN rather than SUCCESS."""
    config_file = write_checks_config(
        [{**_FAILING_CHECK, "include": r"models/does_not_exist/.*\.sql"}]
    )

    result = cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )

    assert result.exit_code == ExitCode.NO_CHECKS_RUN, result.output
    assert "No checks were run." in caplog.text


def test_missing_config_file_exits_config_error(caplog, cli_runner, tmp_path):
    """A `--config-file` that does not exist exits CONFIG_ERROR."""
    result = cli_runner.invoke(
        app,
        ["run", "--config-file", PurePath(tmp_path / "nope.yml").as_posix()],
    )

    assert result.exit_code == ExitCode.CONFIG_ERROR, result.output
    assert "Config file not found" in caplog.text


def test_missing_manifest_exits_artifact_error(
    caplog, cli_runner, tmp_path, write_config
):
    """An artifacts directory without a `manifest.json` exits ARTIFACT_ERROR."""
    empty_target = tmp_path / "target"
    empty_target.mkdir()
    config_file = write_config(
        {
            "dbt_artifacts_dir": str(empty_target),
            "manifest_checks": [_FAILING_CHECK],
        }
    )

    result = cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )

    assert result.exit_code == ExitCode.ARTIFACT_ERROR, result.output
    assert "No manifest.json found" in caplog.text
