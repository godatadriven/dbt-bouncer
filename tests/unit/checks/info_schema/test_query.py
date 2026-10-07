import pytest
from pydantic import ValidationError

from dbt_bouncer.artifact_parsers.info_schema import InfoSchema
from dbt_bouncer.testing import _get_check_class, check_fails, check_passes


def test_query_without_rows_passes(info_schema_dir):
    check_passes(
        "check_info_schema_query",
        sql="SELECT unique_id FROM dbt.models WHERE access = 'private'",
        ctx_info_schema=InfoSchema.from_directory(info_schema_dir),
    )


def test_query_with_rows_fails_and_lists_them(info_schema_dir):
    check_fails(
        "check_info_schema_query",
        match=r"returned 1 row\(s\): unique_id=model\.demo\.customers\.",
        sql="SELECT unique_id FROM dbt.models WHERE description IS NULL",
        ctx_info_schema=InfoSchema.from_directory(info_schema_dir),
    )


def test_long_results_are_truncated(info_schema_dir):
    check_fails(
        "check_info_schema_query",
        match=r"returned 20 row\(s\) \(first 10 shown\)",
        sql="SELECT range AS n FROM range(20)",
        ctx_info_schema=InfoSchema.from_directory(info_schema_dir),
    )


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SELECT 1; SELECT 2", id="two_statements"),
        pytest.param("COPY dbt.models TO 'models.csv'", id="copy"),
        pytest.param("ATTACH 'other.duckdb'", id="attach"),
        pytest.param("SELEC 1", id="syntax_error"),
    ],
)
def test_sql_is_validated_at_config_load(sql):
    with pytest.raises(ValidationError):
        _get_check_class("check_info_schema_query")(
            name="check_info_schema_query", sql=sql
        )
