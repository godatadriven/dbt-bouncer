"""Integration tests for `custom_checks_dir`, end to end through the CLI.

Writing a check in Python and pointing `custom_checks_dir` at it is the
headline extensibility feature, but every existing test for it stops short of a
real run: `tests/unit/test_testing.py` exercises the `check_passes`/`check_fails`
helpers, and `tests/unit/test_config_file_validator.py` only exercises the conf
cache key. Neither proves that a check file on disk is discovered, assembled
into the generated config model, validated against YAML params and executed
against real artifacts.

These tests drive the whole chain: a `.py` file in `tmp_path`, a config that
names it, `dbt-bouncer run`, and the resulting exit code.
"""

from pathlib import PurePath

import pytest

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app
from tests.integration.constants import DBT_112_TARGET, strip_ansi

# The config's `custom_checks_dir` is resolved relative to the config file, so
# tests write the tree under `tmp_path` and reference it by this bare name.
_CUSTOM_CHECKS_DIR_NAME = "my_custom_checks"

# A check that fails unless the model name starts with `prefix`. It declares a
# keyword-only param so the run also proves YAML params reach a custom check.
_CUSTOM_CHECK = '''
from dbt_bouncer.check_framework.decorator import check, fail


@check
def check_model_name_prefix(model, *, prefix: str = "stg_"):
    """Model names must start with the configured prefix."""
    if not str(model.name).startswith(prefix):
        fail(f"`{model.unique_id}` does not start with `{prefix}`.")
'''

# Logged when the config names a check the registry does not hold -- i.e. when
# the custom check was not discovered.
_UNKNOWN_CHECK_ERROR = (
    "Check 'check_model_name_prefix' does not match any of the expected checks."
)

# `orders` lives here and does not start with `stg_`; `stg_orders` does.
_ORDERS = r"models/marts/finance/orders\.sql"
_STG_ORDERS = r"models/staging/crm/stg_orders\.sql"


def _write_custom_check(
    tmp_path,
    body: str = _CUSTOM_CHECK,
    *,
    file_name: str = "check_model_name_prefix.py",
    sub_dir: str = "manifest",
):
    """Write a custom check file into a `custom_checks_dir` tree under `tmp_path`.

    Mirrors the layout documented in `docs/faq.md`: a package directory holding
    an artifact-type subdirectory, which is where the loader's `*/*.py` glob
    looks.

    Returns:
        Path: The custom checks directory.

    """
    custom_dir = tmp_path / _CUSTOM_CHECKS_DIR_NAME
    target_dir = custom_dir / sub_dir if sub_dir else custom_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    (custom_dir / "__init__.py").touch()
    (target_dir / file_name).write_text(body, encoding="utf-8")
    return custom_dir


@pytest.fixture
def custom_checks_config(write_config):
    """Return a factory writing a config that enables a `custom_checks_dir`.

    Returns:
        Callable[..., Path]: Called with the manifest checks (and optionally the
            directory name to put in the config), returns the config file path.

    """

    def _write(checks: list[dict], dir_name: str = _CUSTOM_CHECKS_DIR_NAME):
        return write_config(
            {
                "custom_checks_dir": dir_name,
                "dbt_artifacts_dir": str(DBT_112_TARGET),
                "manifest_checks": checks,
            }
        )

    return _write


def _run(cli_runner, config_file):
    """Invoke `dbt-bouncer run` against `config_file`.

    Returns:
        Result: The `CliRunner` result.

    """
    return cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )


def test_custom_check_is_assembled_and_passes(
    cli_runner, custom_checks_config, tmp_path
):
    """A custom check on disk is discovered, run, and counted as a passing check."""
    _write_custom_check(tmp_path)
    config_file = custom_checks_config(
        [{"name": "check_model_name_prefix", "include": _STG_ORDERS}]
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert "All checks passed! SUCCESS=1 WARN=0 ERROR=0" in strip_ansi(result.output)


def test_custom_check_can_fail(caplog, cli_runner, custom_checks_config, tmp_path):
    """A custom check that calls `fail()` fails the run and reports its message."""
    _write_custom_check(tmp_path)
    config_file = custom_checks_config(
        [{"name": "check_model_name_prefix", "include": _ORDERS}]
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    assert "Done. SUCCESS=0 WARN=0 ERROR=1" in strip_ansi(result.output)
    assert "`dbt-bouncer` failed." in caplog.text


def test_custom_check_honours_yaml_param(cli_runner, custom_checks_config, tmp_path):
    """A keyword-only argument on a custom check is configurable from YAML.

    The default `prefix` would pass on `stg_orders`; overriding it from the
    config is what makes the run fail, so the param demonstrably reached the
    check rather than the default being used.
    """
    _write_custom_check(tmp_path)
    config_file = custom_checks_config(
        [{"name": "check_model_name_prefix", "include": _STG_ORDERS, "prefix": "zzz_"}]
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    assert "Done. SUCCESS=0 WARN=0 ERROR=1" in strip_ansi(result.output)


def test_custom_check_respects_severity_warn(
    cli_runner, custom_checks_config, tmp_path
):
    """Custom checks get the shared `severity` param without declaring it."""
    _write_custom_check(tmp_path)
    config_file = custom_checks_config(
        [{"name": "check_model_name_prefix", "include": _ORDERS, "severity": "warn"}]
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert "Done. SUCCESS=0 WARN=1 ERROR=0" in strip_ansi(result.output)


def test_custom_check_unknown_without_custom_checks_dir(
    caplog, cli_runner, tmp_path, write_config
):
    """Without `custom_checks_dir` the same config is rejected as an unknown check.

    The counterpart to the passing test: it pins that discovery is what the
    config key buys, not something else already importing the file.
    """
    _write_custom_check(tmp_path)
    config_file = write_config(
        {
            "dbt_artifacts_dir": str(DBT_112_TARGET),
            "manifest_checks": [
                {"name": "check_model_name_prefix", "include": _STG_ORDERS}
            ],
        }
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.CONFIG_ERROR, result.output
    assert _UNKNOWN_CHECK_ERROR in caplog.text


def test_custom_checks_dir_that_does_not_exist_warns(
    caplog, cli_runner, custom_checks_config
):
    """A `custom_checks_dir` pointing nowhere warns rather than failing silently."""
    config_file = custom_checks_config(
        [{"name": "check_model_names", "model_name_pattern": "^.*$"}],
        dir_name="not_a_directory",
    )

    _run(cli_runner, config_file)

    assert "does not exist" in caplog.text
    assert "not_a_directory" in caplog.text


def test_custom_check_at_top_level_warns_and_is_not_loaded(
    caplog, cli_runner, custom_checks_config, tmp_path
):
    """A check file directly in `custom_checks_dir` is not discovered, but warns.

    The loader globs `*/*.py`, so a file at the top level is skipped. Without
    the warning the check would silently never run.
    """
    _write_custom_check(tmp_path, sub_dir="")
    config_file = custom_checks_config(
        [{"name": "check_model_name_prefix", "include": _STG_ORDERS}]
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.CONFIG_ERROR, result.output
    assert "were not loaded" in caplog.text
    assert "check_model_name_prefix.py" in caplog.text
    assert _UNKNOWN_CHECK_ERROR in caplog.text


def test_custom_check_that_cannot_import_fails_the_run(
    caplog, cli_runner, custom_checks_config, tmp_path
):
    """A custom check file that raises on import aborts the run.

    Skipping it would leave the run green while the check never executes.
    """
    _write_custom_check(tmp_path, body="import a_module_that_does_not_exist\n")
    config_file = custom_checks_config(
        [{"name": "check_model_names", "model_name_pattern": "^.*$"}]
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.CONFIG_ERROR, result.output
    assert "Failed to load custom check file" in caplog.text
