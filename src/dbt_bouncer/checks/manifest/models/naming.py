"""Checks related to model naming conventions."""

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


@check(code="MO058")
def check_model_alias(
    model, *, alias_pattern: str | None = None, require_explicit_alias: bool = False
):
    """Models must have an explicit alias and/or an alias that matches the supplied regex.

    !!! info "Rationale"

        `check_model_names` governs the file name; the alias governs the table name consumers see in the warehouse. Marts often carry layer suffixes in the file name (e.g. `fct_volume_fraction_ldm`) and expose a clean alias (`FCT_VOLUME_FRACTION`); enforcing this in CI keeps warehouse naming consistent.

    Parameters:
        alias_pattern (str | None): Regexp the model alias must match. If not supplied, the alias is not checked against a pattern.
        require_explicit_alias (bool): If `True`, the model must have an alias explicitly configured (i.e. not just dbt's default of the model name). Default: `False`.

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
            - name: check_model_alias
              include: ^models/marts
              require_explicit_alias: true
              alias_pattern: ^(FCT|DIM)_[A-Z_]+$
        ```

    """
    display_name = get_clean_model_name(model.unique_id)

    # `model.alias` always exists in the manifest (dbt defaults it to `model.name`), so an
    # explicitly configured alias is detected via the (unrendered) config first, falling back
    # to comparing the resolved alias to the model name.
    config_alias = model.config.alias if model.config else None
    unrendered_alias = (
        model.unrendered_config.get("alias") if model.unrendered_config else None
    )
    explicit_alias = (
        config_alias is not None
        or unrendered_alias is not None
        or model.alias != model.name
    )

    if require_explicit_alias and not explicit_alias:
        fail(f"`{display_name}` has no explicit alias configured.")

    if alias_pattern is not None:
        compiled = compile_pattern(alias_pattern.strip())
        if compiled.match(str(model.alias)) is None:
            fail(
                f"`{display_name}` alias `{model.alias}` does not match the supplied regex `{alias_pattern.strip()}`."
            )


@check(code="MO038")
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
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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
