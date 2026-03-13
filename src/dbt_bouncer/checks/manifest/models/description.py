"""Checks related to model descriptions and documentation coverage."""

import re
from pathlib import Path
from typing import Any, cast

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import (
    clean_path_str,
    compile_pattern,
    get_clean_model_name,
    is_description_populated,
)


@check
def check_model_description_contains_regex_pattern(model, *, regexp_pattern: str):
    """Models must have a description that matches the provided pattern.

    Receives:
        model (ModelNode): The ModelNode object to check.
        regexp_pattern (str): The regexp pattern that should match the model description.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_description_contains_regex_pattern
            - regex_pattern: .*pattern_to_match.*
        ```

    """
    compiled = compile_pattern(regexp_pattern.strip(), flags=re.DOTALL)
    if not compiled.match(str(model.description)):
        fail(
            f"""`{get_clean_model_name(model.unique_id)}`'s description "{model.description}" doesn't match the supplied regex: {regexp_pattern}."""
        )


@check
def check_model_description_populated(
    model, *, min_description_length: int | None = None
):
    """Models must have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

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
            - name: check_model_description_populated
        ```
        ```yaml
        manifest_checks:
            - name: check_model_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """
    if not is_description_populated(
        model.description or "", min_description_length or 4
    ):
        fail(
            f"`{get_clean_model_name(model.unique_id)}` does not have a populated description."
        )


@check
def check_model_documentation_coverage(
    ctx, *, min_model_documentation_coverage_pct: int = 100
):
    """Set the minimum percentage of models that have a populated description.

    Parameters:
        min_model_documentation_coverage_pct (float): The minimum percentage of models that must have a populated description.

    Receives:
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_documentation_coverage
              min_model_documentation_coverage_pct: 90
        ```
        ```yaml
        manifest_checks:
            - name: check_model_documentation_coverage
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """
    num_models = len(ctx.models)
    models_with_description = []
    for model in ctx.models:
        if is_description_populated(
            description=model.description or "", min_description_length=4
        ):
            models_with_description.append(model.unique_id)

    num_models_with_descriptions = len(models_with_description)
    model_description_coverage_pct = (num_models_with_descriptions / num_models) * 100

    if model_description_coverage_pct < min_model_documentation_coverage_pct:
        fail(
            f"Only {model_description_coverage_pct}% of models have a populated description, this is less than the permitted minimum of {min_model_documentation_coverage_pct}%."
        )


@check
def check_model_documented_in_same_directory(model):
    """Models must be documented in the same directory where they are defined (i.e. `.yml` and `.sql` files are in the same directory).

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
            - name: check_model_documented_in_same_directory
        ```

    """
    model = cast("Any", model)
    model_sql_path = Path(clean_path_str(model.original_file_path))
    model_sql_dir = model_sql_path.parent.parts

    if not (
        hasattr(model, "patch_path")
        and clean_path_str(model.patch_path or "") is not None
    ):
        fail(f"`{get_clean_model_name(model.unique_id)}` is not documented.")

    patch_path_str = clean_path_str(model.patch_path or "")
    start_idx = patch_path_str.find("models")
    if start_idx != -1:
        patch_path_str = patch_path_str[start_idx:]

    model_doc_path = Path(patch_path_str)
    model_doc_dir = model_doc_path.parent.parts

    if model_doc_dir != model_sql_dir:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is documented in a different directory to the `.sql` file: `{'/'.join(model_doc_dir)}` vs `{'/'.join(model_sql_dir)}`."
        )
