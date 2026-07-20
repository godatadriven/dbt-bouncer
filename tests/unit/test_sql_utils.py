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


def test_parse_sql_caches_results():
    # parse_sql is @lru_cache'd so a repeated parse is reused across callers;
    # assert identity (not just equality) to document and guard that contract.
    assert parse_sql("SELECT id FROM my_table") is parse_sql("SELECT id FROM my_table")


def test_parse_sql_honours_dialect():
    # The dialect argument is passed straight through to sqlglot; BigQuery-style
    # backtick-quoted identifiers must parse when the BigQuery dialect is given.
    parsed = parse_sql("SELECT col FROM `my-project.dataset.table`", dialect="bigquery")
    assert parsed is not None
    assert len(parsed) == 1


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


def test_neutralize_jinja_delimiter_inside_string_is_handled():
    # A `%}` inside a Jinja string must not end the statement early. The old
    # non-greedy regex truncated at the first `%}` and corrupted the SQL; the
    # jinja2 lexer closes the block at the real delimiter.
    neutralized = neutralize_jinja('{% set x = "%}" %}SELECT id FROM t')
    assert "%}" not in neutralized
    assert "_dbt_bouncer_jinja_" not in neutralized
    assert parse_sql(neutralized) is not None


def test_neutralize_jinja_nested_braces_become_single_placeholder():
    # A dict/subscript expression contains inner braces; it must collapse to one
    # placeholder rather than being split.
    neutralized = neutralize_jinja("SELECT {{ {'a': 1}['a'] }} AS c FROM t")
    assert neutralized.count("_dbt_bouncer_jinja_") == 1
    assert "{" not in neutralized
    assert parse_sql(neutralized) is not None


def test_neutralize_jinja_whitespace_control_is_dropped():
    # {%- ... -%} statements render to nothing just like {% ... %}.
    neutralized = neutralize_jinja(
        "SELECT id\n{%- if true -%} , name {%- endif -%}\nFROM t"
    )
    assert "{%" not in neutralized
    assert "_dbt_bouncer_jinja_" not in neutralized
    assert parse_sql(neutralized) is not None


def test_neutralize_jinja_malformed_input_does_not_raise():
    # Unclosed Jinja must degrade gracefully (return a string, no exception) so
    # parse_sql can fail and callers hit their best-effort fallback.
    neutralized = neutralize_jinja("SELECT {{ x FROM t")
    assert isinstance(neutralized, str)


def test_neutralize_jinja_unlexable_input_is_returned_unchanged():
    # Jinja the lexer cannot tokenize at all (an unterminated ``{# ... #}``
    # comment raises ``TemplateSyntaxError``) hits the documented fallback: the
    # input is returned unchanged so parse_sql fails and callers fall back.
    code = "SELECT {# unterminated comment"
    assert neutralize_jinja(code) == code
