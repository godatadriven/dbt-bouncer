"""Checks related to model naming conventions."""

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


@check
def check_model_names(model, *, model_name_pattern: str):
    """Models must have a name that matches the supplied regex.

    !!! info "Rationale"

        Naming conventions such as `stg_` for staging models and `int_` for intermediate models are a cornerstone of readable dbt projects. Enforcing these patterns in CI prevents inconsistently named models from being merged, keeping the project navigable as it grows.

    Parameters:
        model_name_pattern (str): Regexp the model name must match.

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
            - name: check_model_names
              include: ^models/intermediate
              model_name_pattern: ^int_
            - name: check_model_names
              include: ^models/staging
              model_name_pattern: ^stg_
        ```

    """
    compiled = compile_pattern(model_name_pattern.strip())
    if compiled.match(str(model.name)) is None:
        display_name = get_clean_model_name(model.unique_id)
        fail(
            f"`{display_name}` does not match the supplied regex `{model_name_pattern.strip()}`."
        )
