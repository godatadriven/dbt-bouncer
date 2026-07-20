"""SQL parsing helpers built on sqlglot, shared by SQL-structural checks.

These helpers let checks analyse a model's SQL via sqlglot's abstract syntax
tree instead of regular expressions. Model ``raw_code`` contains un-rendered
Jinja (``{{ ref(...) }}``, ``{% ... %}``) which is not valid SQL, so
:func:`neutralize_jinja` replaces those constructs with placeholders before
parsing. :func:`parse_sql` returns ``None`` when parsing fails, allowing callers
to fall back to a best-effort approach.
"""

import re
from functools import lru_cache
from typing import TYPE_CHECKING, cast

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

if TYPE_CHECKING:
    from jinja2 import Environment

# ``JINJA_COMMENT_PATTERN`` stays public so the regex-fallback path in
# ``checks.manifest.models.code`` can reuse the same definition of a Jinja
# comment. Jinja in a model's ``raw_code`` is otherwise neutralized structurally
# by :func:`neutralize_jinja`, which walks jinja2's own token stream rather than
# matching regexes against a delimited language.
JINJA_COMMENT_PATTERN = re.compile(r"\{#.*?#\}", re.DOTALL)


@lru_cache(maxsize=1)
def _get_jinja_lexer_environment() -> "Environment":
    """Build the Jinja environment used to lex model SQL.

    Deferred and cached: jinja2 adds ~25ms to import time. Lexing is
    tag-agnostic, so - unlike ``check_macros``, which *parses* macros and needs
    dbt's custom tags registered - no extensions are required to tokenize
    ``{% materialization %}`` and friends.

    Returns:
        Environment: A Jinja environment used only for its lexer.

    """
    from jinja2 import Environment

    return Environment(autoescape=True)


@lru_cache(maxsize=2048)
def neutralize_jinja(code: str) -> str:
    """Replace Jinja constructs so the SQL is more likely to parse.

    Walks jinja2's own lexer, so delimiters inside strings
    (``{% set x = "%}" %}``), nested braces (``{{ {'a': 1}['a'] }}``) and
    whitespace control (``{%- ... -%}``) are handled correctly. Constructs that
    render to nothing (comments ``{# ... #}``, statements ``{% ... %}`` such as
    ``set``/``if``/``for``, and ``{{ config(...) }}``) become whitespace, while
    every other ``{{ ... }}`` expression becomes a unique single-part
    identifier - so ``{{ ref('x') }}`` never survives as a dotted
    (hard-coded-looking) reference and the surrounding SQL stays structurally
    valid where possible.

    Malformed Jinja that the lexer cannot tokenize is returned unchanged, so
    :func:`parse_sql` fails and callers fall back to their best-effort path.

    Returns:
        The input string with Jinja constructs neutralized.

    """
    from jinja2.exceptions import TemplateSyntaxError
    from jinja2.lexer import (
        TOKEN_BLOCK_END,
        TOKEN_COMMENT,
        TOKEN_DATA,
        TOKEN_NAME,
        TOKEN_VARIABLE_BEGIN,
        TOKEN_VARIABLE_END,
    )

    environment = _get_jinja_lexer_environment()
    try:
        tokens = list(environment.lex(code))
    except TemplateSyntaxError:
        return code

    parts: list[str] = []
    n = 0
    in_variable = False
    variable_first_name: str | None = None
    for _lineno, token_type, value in tokens:
        if token_type == TOKEN_DATA:
            parts.append(value)
        elif token_type == TOKEN_VARIABLE_BEGIN:
            in_variable = True
            variable_first_name = None
        elif token_type == TOKEN_VARIABLE_END:
            if variable_first_name == "config":
                # ``{{ config(...) }}`` renders to nothing; dropping it (rather
                # than placeholdering) keeps a leading config from becoming a
                # bare identifier that would break parsing.
                parts.append(" ")
            else:
                n += 1
                parts.append(f"_dbt_bouncer_jinja_{n}")
            in_variable = False
        elif token_type == TOKEN_BLOCK_END:
            # ``{% ... %}`` statements (set/if/for/...) render to nothing.
            parts.append(" ")
        elif token_type == TOKEN_COMMENT:
            # ``{# ... #}`` comments render to nothing.
            parts.append(" ")
        elif in_variable and variable_first_name is None and token_type == TOKEN_NAME:
            variable_first_name = value

    return "".join(parts)


@lru_cache(maxsize=2048)
def parse_sql(
    code: str, dialect: str | None = None
) -> tuple[exp.Expression, ...] | None:
    """Parse SQL into a tuple of statement ASTs, or ``None`` if it cannot be parsed.

    Results are cached so that SQL parsed by one check is reused by another. The
    returned expressions are shared across callers and must be treated as
    **read-only** - do not mutate the cached AST.

    Args:
        code: The SQL to parse. May contain SQL comments (handled natively by
            sqlglot) but not un-rendered Jinja (neutralize it first).
        dialect: Optional sqlglot dialect. ``None`` parses dialect-agnostically.

    Returns:
        A tuple of parsed statement expressions (empty if ``code`` is blank), or
        ``None`` if sqlglot could not parse the input.

    """
    if not code or not code.strip():
        return ()
    try:
        return cast(
            "tuple[exp.Expression, ...]",
            tuple(e for e in sqlglot.parse(code, dialect=dialect) if e is not None),
        )
    except SqlglotError:
        # SqlglotError is the base of every sqlglot parse/tokenize error, so this
        # catches all parse failures (ParseError, TokenError, ...) while letting
        # genuinely unexpected errors (e.g. MemoryError, RecursionError) surface.
        return None
