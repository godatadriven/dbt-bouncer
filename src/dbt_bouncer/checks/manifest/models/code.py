"""Checks related to model source code content and structure."""

import re

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


@check(
    "check_model_code_does_not_contain_regexp_pattern",
    iterate_over="model",
    params={"regexp_pattern": str},
)
def check_model_code_does_not_contain_regexp_pattern(
    model, ctx, *, regexp_pattern: str
):
    """The raw code for a model must not match the specified regexp pattern."""
    compiled = compile_pattern(regexp_pattern.strip(), flags=re.DOTALL)
    if compiled.match(str(model.raw_code)) is not None:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` contains a banned string: `{regexp_pattern}`."
        )


@check("check_model_hard_coded_references", iterate_over="model")
def check_model_hard_coded_references(model, ctx):
    """A model must not contain hard-coded table references; use ref() or source() instead."""
    jinja_pattern = re.compile(r"\{[{%].*?[%}]\}", re.DOTALL)
    hard_coded_ref_pattern = re.compile(r"\b(?:FROM|JOIN)\s+\w+\.\w+", re.IGNORECASE)

    raw_code = model.raw_code or ""
    cleaned = jinja_pattern.sub("", raw_code)
    matches = hard_coded_ref_pattern.findall(cleaned)
    if matches:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` contains hard-coded table "
            f"references: {matches}. Use `{{{{ ref(...) }}}}` or `{{{{ source(..., ...) }}}}` instead."
        )


@check("check_model_has_semi_colon", iterate_over="model")
def check_model_has_semi_colon(model, ctx):
    """Model may not end with a semi-colon (`;`)."""
    raw_code = (model.raw_code or "").strip()
    if raw_code and raw_code[-1] == ";":
        fail(
            f"`{get_clean_model_name(model.unique_id)}` ends with a semi-colon, this is not permitted."
        )


@check(
    "check_model_max_number_of_lines",
    iterate_over="model",
    params={"max_number_of_lines": (int, 100)},
)
def check_model_max_number_of_lines(model, ctx, *, max_number_of_lines: int):
    """Models may not have more than the specified number of lines."""
    actual_number_of_lines = (model.raw_code or "").count("\n") + 1

    if actual_number_of_lines > max_number_of_lines:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has {actual_number_of_lines} lines, this is more than the maximum permitted number of lines ({max_number_of_lines})."
        )
