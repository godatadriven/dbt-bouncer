"""Integration tests for the `baseline` command and `--baseline` / `--state`."""

from pathlib import PurePath

import orjson

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app
from tests.integration.constants import DBT_112_TARGET, FAILING_CHECK


def test_baseline_then_run_suppresses_all(cli_runner, tmp_path, write_checks_config):
    """A baseline of the current failures makes a re-run pass."""
    config_file = write_checks_config(FAILING_CHECK)
    baseline_file = tmp_path / "baseline.json"

    result = cli_runner.invoke(
        app,
        [
            "baseline",
            "--config-file",
            PurePath(config_file).as_posix(),
            "--output-file",
            PurePath(baseline_file).as_posix(),
        ],
    )
    assert result.exit_code == ExitCode.SUCCESS, result.output
    document = orjson.loads(baseline_file.read_bytes())
    assert len(document["failures"]) > 0

    run_result = cli_runner.invoke(
        app,
        [
            "run",
            "--config-file",
            PurePath(config_file).as_posix(),
            "--baseline",
            PurePath(baseline_file).as_posix(),
        ],
    )
    assert run_result.exit_code == ExitCode.SUCCESS, run_result.output


def test_run_without_baseline_fails(cli_runner, write_checks_config):
    """The same failing config exits non-zero without a baseline."""
    config_file = write_checks_config(FAILING_CHECK)

    result = cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )

    assert result.exit_code == ExitCode.CHECK_ERRORS


def test_missing_baseline_is_config_error(cli_runner, tmp_path, write_checks_config):
    """A missing baseline file exits with CONFIG_ERROR."""
    config_file = write_checks_config(FAILING_CHECK)

    result = cli_runner.invoke(
        app,
        [
            "run",
            "--config-file",
            PurePath(config_file).as_posix(),
            "--baseline",
            PurePath(tmp_path / "nope.json").as_posix(),
        ],
    )

    assert result.exit_code == ExitCode.CONFIG_ERROR


def test_state_against_directory_suppresses_all(cli_runner, write_checks_config):
    """`--state` against the same artifacts directory suppresses every failure."""
    config_file = write_checks_config(FAILING_CHECK)

    result = cli_runner.invoke(
        app,
        [
            "run",
            "--config-file",
            PurePath(config_file).as_posix(),
            "--state",
            DBT_112_TARGET.as_posix(),
        ],
    )

    assert result.exit_code == ExitCode.SUCCESS, result.output


def test_state_empty_directory_names_state_in_error(
    cli_runner, tmp_path, write_checks_config
):
    """`--state` at a directory without artifacts reports `--state` as the cause."""
    config_file = write_checks_config(FAILING_CHECK)
    empty_dir = tmp_path / "empty_target"
    empty_dir.mkdir()

    result = cli_runner.invoke(
        app,
        [
            "run",
            "--config-file",
            PurePath(config_file).as_posix(),
            "--state",
            PurePath(empty_dir).as_posix(),
        ],
    )

    assert result.exit_code == ExitCode.ARTIFACT_ERROR
    assert "--state" in result.output


def test_state_not_a_directory_is_config_error(
    cli_runner, tmp_path, write_checks_config
):
    """`--state` pointing at a non-directory exits with CONFIG_ERROR."""
    config_file = write_checks_config(FAILING_CHECK)

    result = cli_runner.invoke(
        app,
        [
            "run",
            "--config-file",
            PurePath(config_file).as_posix(),
            "--state",
            PurePath(tmp_path / "not-a-dir").as_posix(),
        ],
    )

    assert result.exit_code == ExitCode.CONFIG_ERROR
