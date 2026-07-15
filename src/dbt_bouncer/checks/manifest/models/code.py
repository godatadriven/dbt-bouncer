"""Checks related to model source code content and structure."""

import re

from sqlglot import exp

from dbt_bouncer.artifact_types import ModelNode
from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.enums import Materialization
from dbt_bouncer.sql_utils import JINJA_COMMENT_PATTERN, neutralize_jinja, parse_sql
from dbt_bouncer.utils import compile_pattern, get_clean_model_name

# Patterns retained for the best-effort regex fallback used when sqlglot cannot
# parse a model's SQL (e.g. heavy Jinja control flow).
_JINJA_PATTERN = re.compile(r"\{[{%].*?[%}]\}", re.DOTALL)
_HARD_CODED_REF_PATTERN = re.compile(r"\b(?:FROM|JOIN)\s+\w+\.\w+", re.IGNORECASE)
_SELECT_STAR_PATTERN = re.compile(r"(?i)select\s+(?:all\s+|distinct\s+)?\*")

# Patterns used to strip comment forms before the select-star regex fallback.
# The Jinja-comment pattern is shared with ``sql_utils`` to keep a single source
# of truth for what a Jinja comment looks like.
_BLOCK_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_PATTERN = re.compile(r"--[^\n]*")


def _strip_sql_comments(code: str) -> str:
    """Remove SQL and Jinja comment forms from SQL code (regex fallback only).

    Strips Jinja block comments ({# ... #}), SQL block comments (/* ... */),
    and SQL line comments (-- to end of line) so that a ``SELECT *`` inside a
    comment does not trigger the select-star regex fallback.

    Returns:
        The input string with all comment forms removed.

    Note:
        This helper is only used on the fallback path (when sqlglot cannot parse
        the model). It does not account for SQL string literals; the AST path
        does not have that limitation.

    """
    code = JINJA_COMMENT_PATTERN.sub("", code)
    code = _BLOCK_COMMENT_PATTERN.sub("", code)
    code = _LINE_COMMENT_PATTERN.sub("", code)
    return code


def _is_sql_model(model: ModelNode) -> bool:
    """Determine whether a model is a SQL model (non-SQL models are skipped).

    Returns:
        ``True`` if the model's language is SQL (or unset), ``False`` otherwise.

    """
    return (getattr(model, "language", None) or "sql") == "sql"


def _select_uses_star(select: exp.Select) -> bool:
    """Determine whether a ``SELECT`` projects a star.

    Returns:
        ``True`` if any top-level projection is a bare (``*``) or qualified
        (``t.*``) star, ``False`` otherwise.

    """
    for projection in select.expressions:
        if isinstance(projection, exp.Star):
            return True
        if isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star):
            return True
    return False


def _hard_coded_tables(statements: tuple[exp.Expression, ...]) -> list[str]:
    """Find schema/catalog-qualified table references in parsed SQL.

    Returns:
        The qualified table references (e.g. ``schema.table``), in first-seen
        order and de-duplicated.

    """
    seen: set[str] = set()
    tables: list[str] = []
    for statement in statements:
        for table in statement.find_all(exp.Table):
            if table.args.get("db") or table.args.get("catalog"):
                rendered = table.sql()
                if rendered not in seen:
                    seen.add(rendered)
                    tables.append(rendered)
    return tables


@check
def check_model_code_does_not_contain_regexp_pattern(model, *, regexp_pattern: str):
    """The raw code for a model must not match the specified regexp pattern.

    !!! info "Rationale"

        Teams often adopt coding standards that forbid certain SQL patterns — for example, using `ifnull` instead of `coalesce`, or using deprecated functions. This check allows those standards to be enforced automatically at CI time, preventing non-compliant code from reaching production.

    Parameters:
        regexp_pattern (str): The regexp pattern that should not be matched by the model code.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            # Prefer `coalesce` over `ifnull`: https://docs.sqlfluff.com/en/stable/rules.html#sqlfluff.rules.sphinx.Rule_CV02
            - name: check_model_code_does_not_contain_regexp_pattern
              regexp_pattern: .*[i][f][n][u][l][l].*
        ```

    """
    compiled = compile_pattern(regexp_pattern.strip(), flags=re.DOTALL)
    if compiled.match(str(model.raw_code)) is not None:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` contains a banned string: `{regexp_pattern}`."
        )


@check
def check_model_does_not_use_select_star(model):
    """Models must not use `SELECT *`.

    !!! info "Rationale"

        `SELECT *` makes a model's output schema implicit and brittle to upstream column changes. When a source or upstream model adds, removes, or reorders columns, a `SELECT *` model silently propagates the change, potentially breaking downstream consumers or introducing unexpected columns into the DAG. Explicit column lists are self-documenting, stable, and make schema changes intentional and reviewable.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    !!! info

        Analysis is AST-based (via sqlglot) and flags both bare (`SELECT *`) and
        qualified (`SELECT t.*`) stars, while correctly ignoring stars inside
        function calls (`count(*)`), string literals, and comments. It analyses
        the model's `raw_code` with Jinja neutralized. Non-SQL (e.g. Python)
        models are skipped.

    !!! warning

        Models that sqlglot cannot parse (e.g. heavy `{% ... %}` control flow)
        fall back to a best-effort regular-expression scan, which retains the
        original limitations around string literals and Jinja tags.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_does_not_use_select_star
              include: ^models/marts
        ```

    """
    if not _is_sql_model(model):
        return

    parsed = parse_sql(neutralize_jinja(model.raw_code or ""))
    if parsed is None:
        # Fallback: best-effort regex on comment-stripped raw code.
        cleaned = _strip_sql_comments(model.raw_code or "")
        if _SELECT_STAR_PATTERN.search(cleaned):
            fail(
                f"`{get_clean_model_name(model.unique_id)}` uses `SELECT *`; list columns explicitly."
            )
        return

    for statement in parsed:
        for select in statement.find_all(exp.Select):
            if _select_uses_star(select):
                fail(
                    f"`{get_clean_model_name(model.unique_id)}` uses `SELECT *`; list columns explicitly."
                )


@check
def check_model_hard_coded_references(model):
    """A model must not contain hard-coded table references; use ref() or source() instead.

    Flags table references qualified with a schema or catalog (e.g.
    ``FROM schema.table`` or ``JOIN catalog.schema.table``). Hard-coded
    references bypass the dbt DAG, break lineage, and are environment-specific.

    !!! info "Rationale"

        Hard-coded table references bypass dbt's dependency graph, break lineage tracking, and are environment-specific — a reference that works in production will silently read the wrong data in development. Using `ref()` or `source()` ensures models run in the correct order, compile to the right environment, and appear correctly in lineage tools.

    !!! info

        Analysis is AST-based (via sqlglot) on the model's `raw_code` with Jinja
        neutralized, so that `{{ ref(...) }}` / `{{ source(...) }}` are not
        mistaken for hard-coded references. Compiled SQL is intentionally *not*
        used: dbt renders every `ref()`/`source()` into a physical
        schema-qualified relation, which is indistinguishable from a hand-written
        hard-coded reference. Non-SQL (e.g. Python) models are skipped.

    !!! warning

        Models that sqlglot cannot parse (e.g. heavy `{% ... %}` control flow)
        fall back to a best-effort regular-expression scan, which may miss
        references inside complex Jinja logic.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_hard_coded_references
        ```

    """
    if not _is_sql_model(model):
        return

    parsed = parse_sql(neutralize_jinja(model.raw_code or ""))
    if parsed is None:
        # Fallback: best-effort regex on Jinja-stripped raw code.
        cleaned = _JINJA_PATTERN.sub("", model.raw_code or "")
        matches = _HARD_CODED_REF_PATTERN.findall(cleaned)
        if matches:
            fail(
                f"`{get_clean_model_name(model.unique_id)}` contains hard-coded table "
                f"references: {matches}. Use `{{{{ ref(...) }}}}` or `{{{{ source(..., ...) }}}}` instead."
            )
        return

    tables = _hard_coded_tables(parsed)
    if tables:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` contains hard-coded table "
            f"references: {tables}. Use `{{{{ ref(...) }}}}` or `{{{{ source(..., ...) }}}}` instead."
        )


@check
def check_model_has_semi_colon(model):
    """Model may not end with a semi-colon (`;`).

    !!! info "Rationale"

        dbt automatically wraps model SQL before executing it, so a trailing semi-colon can cause syntax errors in certain warehouse adapters. This check catches the mistake at lint time, preventing obscure build failures that can be hard to diagnose in CI.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_semi_colon
              include: ^models/marts
        ```

    """
    raw_code = (model.raw_code or "").strip()
    if raw_code and raw_code[-1] == ";":
        fail(
            f"`{get_clean_model_name(model.unique_id)}` ends with a semi-colon, this is not permitted."
        )


@check
def check_model_incremental_has_unique_key(model):
    """Incremental models must declare a `unique_key`.

    !!! info "Rationale"

        Incremental models without a `unique_key` perform insert-only loads.
        Late-arriving or updated rows are silently duplicated on every run,
        leading to over-counting in downstream aggregations and broken
        idempotency. Declaring a `unique_key` enables dbt to merge or
        delete-insert matching rows, keeping the table correct across reruns.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_incremental_has_unique_key
        ```

    """
    config = model.config
    if (
        config
        and config.materialized == "incremental"
        and not getattr(config, "unique_key", None)
    ):
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is incremental but has no `unique_key`."
        )


@check
def check_model_materialization_permitted(
    model, *, permitted_materializations: list[Materialization]
):
    """Models must use a permitted materialization for their location.

    !!! info "Rationale"

        A project may declare a directory-wide materialization in `dbt_project.yml`
        (e.g. all `staging` models are views), but that default can be silently
        overridden by an in-model `config()` block or a nested properties file.
        Asserting the resolved materialization directly - rather than comparing the
        sources of configuration - catches any model whose final materialization was
        overridden away from what the directory is supposed to guarantee.

    Parameters:
        permitted_materializations (list[Materialization]): List of materializations that models are permitted to use, e.g. `ephemeral`, `incremental`, `table`, `view`.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_materialization_permitted
              include: ^models/staging
              permitted_materializations:
                - view
        ```

    """
    materialized = model.config.materialized if model.config else None
    if materialized not in permitted_materializations:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is materialized as "
            f"`{materialized}`, expected one of "
            f"{[m.value for m in permitted_materializations]}."
        )


@check
def check_model_max_number_of_lines(model, *, max_number_of_lines: int = 100):
    """Models may not have more than the specified number of lines.

    !!! info "Rationale"

        Very long SQL files are a code smell that often indicates a model is doing too much — mixing staging, joining, and aggregating in a single file. Capping line counts encourages splitting large transformations into smaller, focused models that are easier to test, understand, and reuse.

    Parameters:
        max_number_of_lines (int): The maximum number of permitted lines.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_number_of_lines
        ```
        ```yaml
        manifest_checks:
            - name: check_model_max_number_of_lines
              max_number_of_lines: 150
        ```

    """
    if max_number_of_lines <= 0:
        raise ValueError(
            f"`max_number_of_lines` must be greater than 0, got {max_number_of_lines}."
        )

    actual_number_of_lines = (model.raw_code or "").count("\n") + 1

    if actual_number_of_lines > max_number_of_lines:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has {actual_number_of_lines} lines, this is more than the maximum permitted number of lines ({max_number_of_lines})."
        )
