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
from typing import cast

import sqlglot
from sqlglot import exp

# Constructs that render to nothing in real dbt are dropped so the surrounding
# SQL still parses: comments (``{# ... #}``), statements (``{% ... %}`` - set,
# if/for/macro control flow) and ``{{ config(...) }}``. Remaining ``{{ ... }}``
# expressions occupy a value/table position, so each becomes a single-part
# placeholder identifier - notably ``{{ ref('x') }}`` never survives as a
# dotted (hard-coded-looking) reference.
_JINJA_COMMENT_PATTERN = re.compile(r"\{#.*?#\}", re.DOTALL)
_JINJA_STATEMENT_PATTERN = re.compile(r"\{%.*?%\}", re.DOTALL)
_JINJA_CONFIG_PATTERN = re.compile(r"\{\{[-\s]*config\b.*?\}\}", re.DOTALL)
_JINJA_EXPRESSION_PATTERN = re.compile(r"\{\{.*?\}\}", re.DOTALL)


def neutralize_jinja(code: str) -> str:
    """Replace Jinja constructs so the SQL is more likely to parse.

    Constructs that render to nothing (comments ``{# ... #}``, statements
    ``{% ... %}`` such as ``set``/``if``/``for``, and ``{{ config(...) }}``) are
    replaced with whitespace. Remaining ``{{ ... }}`` expressions are each
    replaced with a unique single-part identifier, keeping the surrounding SQL
    structurally valid where possible.

    Returns:
        The input string with Jinja constructs neutralized.

    """
    code = _JINJA_COMMENT_PATTERN.sub(" ", code)
    code = _JINJA_STATEMENT_PATTERN.sub(" ", code)
    code = _JINJA_CONFIG_PATTERN.sub(" ", code)
    n = 0

    def _placeholder(_match: re.Match[str]) -> str:
        nonlocal n
        n += 1
        return f"_dbt_bouncer_jinja_{n}"

    return _JINJA_EXPRESSION_PATTERN.sub(_placeholder, code)


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
    except Exception:  # sqlglot raises ParseError/TokenError and similar.
        return None
