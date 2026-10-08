"""Model grain checks that use the dbt Information Schema."""

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import get_clean_model_name


@check(code="IS006")
def check_model_grain_is_tested(model, ctx):
    """The grain of a model must be covered by a uniqueness test.

    !!! info "Rationale"

        The grain is the set of columns that identifies one row of a model. Downstream joins and aggregations assume it holds: when it does not, duplicate rows silently inflate metrics. dbt records both the grain of each model and the columns that a uniqueness test covers, so this check can fail when a model declares a grain that no test enforces.

    !!! note

        This check requires the dbt Information Schema (dbt 2.0+, `--generate-info-schema`). Models without a grain, and models missing from the Information Schema, are not checked: use `check_model_has_grain` to require a grain.

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
        info_schema_checks:
            - name: check_model_grain_is_tested
              include: ^models/marts
        ```

    """
    row = ctx.info_schema.models_by_unique_id.get(model.unique_id)
    if row is None or not row.get("grain"):
        return
    grain = {c.casefold() for c in row["grain"]}
    tested = {c.casefold() for c in row.get("grain_tested") or []}
    if grain != tested:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has the grain {sorted(row['grain'])} but its uniqueness tests cover {sorted(row.get('grain_tested') or [])}."
        )


@check(code="IS007")
def check_model_has_grain(model, ctx):
    """Models must have a grain.

    !!! info "Rationale"

        A model without a known grain gives consumers no way to tell what one row represents, and no way to join to it safely. dbt derives the grain of each model from its configuration and its uniqueness tests, so a model without a grain usually lacks a primary key test.

    !!! note

        This check requires the dbt Information Schema (dbt 2.0+, `--generate-info-schema`). Models missing from the Information Schema are not checked.

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
        info_schema_checks:
            - name: check_model_has_grain
              include: ^models/marts
        ```

    """
    row = ctx.info_schema.models_by_unique_id.get(model.unique_id)
    if row is not None and not row.get("grain"):
        fail(f"`{get_clean_model_name(model.unique_id)}` has no grain.")
