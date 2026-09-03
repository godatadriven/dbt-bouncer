"""Integration tests for the baseline command and `--baseline` / `--state`."""

from pathlib import Path, PurePath

import orjson
import yaml
from typer.testing import CliRunner

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app

FIXTURE_TARGET = (Path("./tests/fixtures/dbt_112/target")).absolute()


def _write_config(tmp_path, checks):
    config = {"dbt_artifacts_dir": str(FIXTURE_TARGET), "manifest_checks": checks}
    config_file = tmp_path / "dbt-bouncer.yml"
    with config_file.open("w", encoding="utf-8") as f:
        yaml.dump(config, f)
    return config_file


# A check that fails on every model: no model name starts with "zzz_".
_FAILING_CHECK = [{"name": "check_model_names", "model_name_pattern": "^zzz_"}]


def test_baseline_then_run_suppresses_all(tmp_path):
    """A baseline of the current failures makes a re-run pass."""
    config_file = _write_config(tmp_path, _FAILING_CHECK)
    baseline_file = tmp_path / "baseline.json"
    runner = CliRunner()

    result = runner.invoke(
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

    run_result = runner.invoke(
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


def test_run_without_baseline_fails(tmp_path):
    """The same failing config exits non-zero without a baseline."""
    config_file = _write_config(tmp_path, _FAILING_CHECK)
    result = CliRunner().invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )
    assert result.exit_code == ExitCode.CHECK_ERRORS


def test_missing_baseline_is_config_error(tmp_path):
    """A missing baseline file exits with CONFIG_ERROR."""
    config_file = _write_config(tmp_path, _FAILING_CHECK)
    result = CliRunner().invoke(
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


def test_state_against_directory_suppresses_all(tmp_path):
    """`--state` against the same artifacts directory suppresses every failure."""
    config_file = _write_config(tmp_path, _FAILING_CHECK)
    result = CliRunner().invoke(
        app,
        [
            "run",
            "--config-file",
            PurePath(config_file).as_posix(),
            "--state",
            FIXTURE_TARGET.as_posix(),
        ],
    )
    assert result.exit_code == ExitCode.SUCCESS, result.output


def test_state_empty_directory_names_state_in_error(tmp_path):
    """`--state` at a directory without artifacts reports `--state` as the cause."""
    config_file = _write_config(tmp_path, _FAILING_CHECK)
    empty_dir = tmp_path / "empty_target"
    empty_dir.mkdir()
    result = CliRunner().invoke(
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


def test_state_not_a_directory_is_config_error(tmp_path):
    """`--state` pointing at a non-directory exits with CONFIG_ERROR."""
    config_file = _write_config(tmp_path, _FAILING_CHECK)
    result = CliRunner().invoke(
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
