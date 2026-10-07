"""Integration tests for `info_schema_checks` against the dbt 2.0 fixture.

The fixture's `info_schema/v1` is written by `mise run build-artifacts-20` with
`--static-analysis strict --generate-info-schema`. The shipped example config
does not use this category, because CI also runs that config against dbt 1.x
artifacts, which have no Information Schema.
"""

from pathlib import PurePath

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app
from tests.integration.constants import DBT_112_TARGET, FIXTURES_DIR, strip_ansi

DBT_20_TARGET = FIXTURES_DIR / "dbt_20" / "target"
_ORDERS = r"models/marts/finance/orders\.sql"
_STG_ORDERS = r"models/staging/crm/stg_orders\.sql"

# Checks that hold for every model in the fixture.
_PASSING_CHECKS = [
    {
        "name": "check_info_schema_query",
        "sql": "SELECT unique_id FROM dbt.models WHERE name = 'does_not_exist'",
    },
    {"name": "check_model_column_descriptions_propagated"},
    {"name": "check_model_column_meta_propagated", "meta_key": "pii"},
    {"name": "check_model_columns_have_lineage"},
    {"name": "check_model_grain_is_tested", "include": _STG_ORDERS},
    {"name": "check_model_has_grain", "include": _STG_ORDERS},
    {"name": "check_model_public_columns_not_derived_from_meta", "meta_key": "pii"},
    {"name": "check_source_columns_are_used"},
]


def _run(cli_runner, config_file):
    """Invoke `dbt-bouncer run` against `config_file`.

    Returns:
        Result: The `CliRunner` result.

    """
    return cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )


def test_passing_checks_exit_success(cli_runner, write_config):
    config_file = write_config(
        {"dbt_artifacts_dir": str(DBT_20_TARGET), "info_schema_checks": _PASSING_CHECKS}
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert "All checks passed!" in strip_ansi(result.output)


def test_type_mismatch_in_fixture_fails(cli_runner, tmp_path, write_config):
    """`orders` declares `double` amounts that its SQL produces as integers.

    dbt itself reports these as `dbt1058` under `--static-analysis strict`.
    """
    output_file = tmp_path / "results.json"
    config_file = write_config(
        {
            "dbt_artifacts_dir": str(DBT_20_TARGET),
            "info_schema_checks": [
                {"name": "check_model_column_types_match_inferred", "include": _ORDERS}
            ],
        }
    )

    result = cli_runner.invoke(
        app,
        [
            "run",
            "--config-file",
            PurePath(config_file).as_posix(),
            "--output-file",
            PurePath(output_file).as_posix(),
        ],
    )

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    assert "Done. SUCCESS=0 WARN=0 ERROR=1" in strip_ansi(result.output)
    assert "`amount` (declared `double`, inferred `Int64`)" in output_file.read_text()


def test_missing_info_schema_exits_artifact_error(caplog, cli_runner, write_config):
    """The dbt 1.12 fixture has no Information Schema, so the category cannot run."""
    config_file = write_config(
        {
            "dbt_artifacts_dir": str(DBT_112_TARGET),
            "info_schema_checks": [{"name": "check_model_has_grain"}],
        }
    )

    result = _run(cli_runner, config_file)

    assert result.exit_code == ExitCode.ARTIFACT_ERROR, result.output
    assert "No dbt Information Schema found at" in caplog.text
