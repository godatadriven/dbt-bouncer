import pytest

from dbt_bouncer.sql_utils import neutralize_jinja, parse_sql


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        pytest.param("", (), id="empty"),
        pytest.param("   \n  ", (), id="whitespace_only"),
    ],
)
def test_parse_sql_blank_returns_empty_tuple(code, expected):
    assert parse_sql(code) == expected


def test_parse_sql_valid_returns_statements():
    parsed = parse_sql("SELECT id FROM my_table")
    assert parsed is not None
    assert len(parsed) == 1


def test_parse_sql_multiple_statements():
    parsed = parse_sql("SELECT 1; SELECT 2")
    assert parsed is not None
    assert len(parsed) == 2


def test_parse_sql_invalid_returns_none():
    assert parse_sql("this is (((not sql at all !!!") is None


def test_neutralize_jinja_expression_becomes_single_part_identifier():
    # A ref() expression must not survive as a dotted (hard-coded-looking) reference.
    neutralized = neutralize_jinja("SELECT * FROM {{ ref('other_model') }}")
    assert "{{" not in neutralized
    assert "." not in neutralized.split("FROM", 1)[1]


def test_neutralize_jinja_drops_comments():
    neutralized = neutralize_jinja("{# a comment #}\nSELECT id FROM t")
    assert "{#" not in neutralized
    assert "comment" not in neutralized


def test_neutralize_jinja_statement_is_dropped():
    # {% ... %} renders to nothing in dbt, so it must not leave a dangling token.
    neutralized = neutralize_jinja("{% set x = 1 %}SELECT id FROM t")
    assert "{%" not in neutralized
    assert "_dbt_bouncer_jinja_" not in neutralized
    assert parse_sql(neutralized) is not None


def test_neutralize_jinja_leading_config_does_not_break_parsing():
    # A leading {{ config(...) }} renders empty; it must not become an identifier.
    neutralized = neutralize_jinja(
        '{{ config(materialized="view") }}\nSELECT id FROM t'
    )
    assert "_dbt_bouncer_jinja_" not in neutralized
    assert parse_sql(neutralized) is not None
