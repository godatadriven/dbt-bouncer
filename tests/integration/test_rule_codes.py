"""Integration tests for rule codes, end to end through the CLI.

`docs/checks/rule_codes.md` documents a rule code as a second way to address a
check: it stands in for the name in a config entry, and it is accepted in a
resource's `dbt-bouncer.skip_checks` meta. Both paths are covered at unit level
only -- `tests/unit/test_rule_codes.py` calls `validate_conf` and
`_check_applies_to_resource` directly, and `tests/unit/test_runner.py` builds
the resource facts by hand. Nothing proves that a code written in a config file
survives config validation, check assembly and the match phase of a real run.

These tests drive the whole chain: a config that addresses a check by code, a
manifest whose model meta names that code, `dbt-bouncer run`, and the resulting
counts.
"""

from pathlib import PurePath

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app
from tests.integration.constants import strip_ansi

# `check_model_names`, per `docs/checks/rule_codes.md`.
_CHECK_MODEL_NAMES_CODE = "MO038"

# A code of the right shape that no check carries. Used as a negative control.
_UNUSED_CODE = "MO999"

# Two models with opposite outcomes under the check below: `orders` fails the
# `^stg_` pattern and `stg_orders` passes it. Keeping a passing check in every
# run means a skipped check cannot be confused with a run that did nothing.
_ORDERS = r"models/marts/finance/orders\.sql"
_STG_ORDERS = r"models/staging/crm/stg_orders\.sql"
_NAME_CHECK_PARAMS = {
    "include": [_ORDERS, _STG_ORDERS],
    "model_name_pattern": "^stg_",
}


def _run(cli_runner, config_file):
    """Invoke `dbt-bouncer run` against `config_file`.

    Returns:
        Result: The `CliRunner` result.

    """
    return cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )


def test_check_runs_when_configured_by_rule_code(cli_runner, write_checks_config):
    """A config entry that names a check by its rule code runs that check.

    The counterpart of `test_rule_code_config_validation`, which stops at the
    validated config object: this asserts the resolved check reaches the
    executor and reports a result.
    """
    config_file = write_checks_config(
        [{"code": _CHECK_MODEL_NAMES_CODE, **_NAME_CHECK_PARAMS}]
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    assert "Done. SUCCESS=1 WARN=0 ERROR=1" in strip_ansi(result.output)


def test_resource_skips_check_by_rule_code(
    artifacts_with_skip_checks, cli_runner, write_checks_config
):
    """A model's `skip_checks` meta skips a check named by its rule code.

    Without the code match the run reports one failure, so the skip is what the
    assertion measures.
    """
    artifacts_dir = artifacts_with_skip_checks({"orders": [_CHECK_MODEL_NAMES_CODE]})
    config_file = write_checks_config(
        [{"name": "check_model_names", **_NAME_CHECK_PARAMS}], artifacts_dir
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert "All checks passed! SUCCESS=1 WARN=0 ERROR=0" in strip_ansi(result.output)


def test_resource_skip_checks_ignores_an_unrelated_rule_code(
    artifacts_with_skip_checks, cli_runner, write_checks_config
):
    """A `skip_checks` code that no configured check carries skips nothing.

    Pins that the skip matches the check's own code rather than any code at all.
    """
    artifacts_dir = artifacts_with_skip_checks({"orders": [_UNUSED_CODE]})
    config_file = write_checks_config(
        [{"name": "check_model_names", **_NAME_CHECK_PARAMS}], artifacts_dir
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    assert "Done. SUCCESS=1 WARN=0 ERROR=1" in strip_ansi(result.output)
