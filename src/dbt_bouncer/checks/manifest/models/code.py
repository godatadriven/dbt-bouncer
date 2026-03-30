"""Checks related to model source code content and structure."""

import re

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern, get_clean_model_name

_JINJA_PATTERN = re.compile(r"\{[{%].*?[%}]\}", re.DOTALL)
_HARD_CODED_REF_PATTERN = re.compile(r"\b(?:FROM|JOIN)\s+\w+\.\w+", re.IGNORECASE)


@check
def check_model_code_does_not_contain_regexp_pattern(model, *, regexp_pattern: str):
    """The raw code for a model must not match the specified regexp pattern.

    Parameters:
        regexp_pattern (str): The regexp pattern that should not be matched by the model code.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
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
def check_model_hard_coded_references(model):
    """A model must not contain hard-coded table references; use ref() or source() instead.

    Scans ``raw_code`` for patterns like ``FROM schema.table`` or
    ``JOIN catalog.schema.table`` that are not wrapped in Jinja expressions.
    Hard-coded references bypass the dbt DAG, break lineage, and are
    environment-specific.

    !!! warning

        This check is not foolproof and will not catch all hard-coded table
        references (e.g. references inside complex Jinja logic or comments).

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_hard_coded_references
        ```

    """
    raw_code = model.raw_code or ""
    cleaned = _JINJA_PATTERN.sub("", raw_code)
    matches = _HARD_CODED_REF_PATTERN.findall(cleaned)
    if matches:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` contains hard-coded table "
            f"references: {matches}. Use `{{{{ ref(...) }}}}` or `{{{{ source(..., ...) }}}}` instead."
        )


@check
def check_model_has_semi_colon(model):
    """Model may not end with a semi-colon (`;`).

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
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
def check_model_max_number_of_lines(model, *, max_number_of_lines: int = 100):
    """Models may not have more than the specified number of lines.

    Parameters:
        max_number_of_lines (int): The maximum number of permitted lines.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
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
            f"`max_number_of_lines` must be positive, got {max_number_of_lines}."
        )

    actual_number_of_lines = (model.raw_code or "").count("\n") + 1

    if actual_number_of_lines > max_number_of_lines:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has {actual_number_of_lines} lines, this is more than the maximum permitted number of lines ({max_number_of_lines})."
        )
