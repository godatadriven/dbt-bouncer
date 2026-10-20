import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

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


_SQL_IDENT = st.from_regex(r"[a-z][a-z0-9_]{0,12}", fullmatch=True).filter(
    lambda s: (
        s
        not in {
            "add",
            "all",
            "alter",
            "and",
            "any",
            "as",
            "asc",
            "between",
            "by",
            "case",
            "cast",
            "check",
            "column",
            "commit",
            "constraint",
            "create",
            "cross",
            "current",
            "default",
            "delete",
            "desc",
            "distinct",
            "drop",
            "else",
            "end",
            "escape",
            "except",
            "exists",
            "false",
            "fetch",
            "for",
            "foreign",
            "from",
            "full",
            "function",
            "grant",
            "group",
            "having",
            "if",
            "in",
            "index",
            "inner",
            "insert",
            "intersect",
            "into",
            "is",
            "join",
            "key",
            "left",
            "like",
            "limit",
            "natural",
            "not",
            "null",
            "offset",
            "on",
            "or",
            "order",
            "outer",
            "over",
            "partition",
            "primary",
            " qualifiers",
            "references",
            "revoke",
            "right",
            "rollback",
            "row",
            "rows",
            "select",
            "set",
            "some",
            "table",
            "then",
            "to",
            " trigger",
            "true",
            "truncate",
            "union",
            "unique",
            "update",
            "using",
            "values",
            "view",
            "when",
            "where",
            "window",
            "with",
        }
    )
)

_JINJA_EXPR = st.one_of(
    _SQL_IDENT.map(lambda c: f"{{{{ {c} }}}}"),
    _SQL_IDENT.map(lambda c: f"{{{{ my_macro('{c}') }}}}"),
    _SQL_IDENT.map(lambda c: f"{{{{ {{'{c}': 1}}['{c}'] }}}}"),
)


@st.composite
def _templated_sql_strategy(draw: st.DrawFn) -> str:
    parts: list[str] = []
    if draw(st.booleans()):
        mat = draw(st.sampled_from(["ephemeral", "incremental", "table", "view"]))
        parts.append(f'{{{{ config(materialized="{mat}") }}}}')
    if draw(st.booleans()):
        comment_body = draw(
            st.text(
                alphabet=st.characters(blacklist_characters="{#}%\r\n"),
                max_size=25,
            )
        )
        parts.append(f"{{# {comment_body} #}}")
    if draw(st.booleans()):
        tricky_lit = draw(st.sampled_from(["plain", "%}", "}}", "{%", "{{"]))
        parts.append(f'{{% set x = "{tricky_lit}" %}}')

    cols = draw(
        st.lists(
            st.one_of(_SQL_IDENT, _JINJA_EXPR),
            min_size=1,
            max_size=4,
        )
    )
    select_clause = "SELECT " + ", ".join(cols)
    if draw(st.booleans()):
        extra_col = draw(_SQL_IDENT)
        select_clause += f"\n{{%- if true -%}} , {extra_col} {{%- endif -%}}"

    model_ref = draw(_SQL_IDENT)
    rel_expr = draw(
        st.sampled_from(
            [
                f"{{{{ ref('{model_ref}') }}}}",
                f"{{{{ source('raw', '{model_ref}') }}}}",
            ]
        )
    )
    parts.append(f"{select_clause}\nFROM {rel_expr}")
    return "\n".join(parts)


class TestNeutralizeJinjaProperties:
    """Property-based tests for Jinja neutralization."""

    @settings(max_examples=50)
    @given(st.text(alphabet=st.characters(blacklist_characters="{}%#\r\n")))
    def test_identity_on_clean_sql(self, sql: str):
        """Clean SQL without Jinja constructs or newlines is unchanged."""
        assert neutralize_jinja(sql) == sql

    @settings(max_examples=100)
    @given(st.text())
    def test_never_crashes_on_arbitrary_fuzzed_text(self, text: str):
        """neutralize_jinja must never raise unhandled exceptions and is idempotent."""
        result = neutralize_jinja(text)
        assert isinstance(result, str)
        assert neutralize_jinja(result) == result

    @settings(max_examples=50)
    @given(sql=_templated_sql_strategy())
    def test_templated_sql_neutralizes_delimiters_and_parses(self, sql: str):
        """Well-formed Jinja SQL has all delimiters stripped and parses into an AST."""
        neutralized = neutralize_jinja(sql)
        for delimiter in ("{{", "}}", "{%", "%}", "{#", "#}"):
            assert delimiter not in neutralized
        assert neutralize_jinja(neutralized) == neutralized

        parsed = parse_sql(neutralized)
        assert parsed is not None
        assert len(parsed) == 1
        assert parse_sql(neutralized) is parsed
