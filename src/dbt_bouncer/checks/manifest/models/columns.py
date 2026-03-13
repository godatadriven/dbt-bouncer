"""Checks related to model column definitions, types, and constraints."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.checks.common import NestedDict
from dbt_bouncer.enums import Materialization
from dbt_bouncer.utils import find_missing_meta_keys, get_clean_model_name


@check(
    "check_model_columns_have_meta_keys",
    iterate_over="model",
    params={"keys": NestedDict},
)
def check_model_columns_have_meta_keys(model, ctx, *, keys: NestedDict):
    """Columns defined for models must have the specified keys in the `meta` config."""
    columns = model.columns or {}
    failing_columns: dict[str, list[str]] = {}
    for col_name, col in columns.items():
        missing_keys = find_missing_meta_keys(
            meta_config=col.meta or {},
            required_keys=keys.model_dump(),
        )
        if missing_keys:
            failing_columns[col_name] = [k.replace(">>", "") for k in missing_keys]
    if failing_columns:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has columns missing required `meta` keys: {failing_columns}"
        )


@check("check_model_columns_have_types", iterate_over="model")
def check_model_columns_have_types(model, ctx):
    """Columns defined for models must have a `data_type` declared."""
    columns = model.columns or {}
    untyped_columns = [
        col_name for col_name, col in columns.items() if not col.data_type
    ]
    if untyped_columns:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has columns without a declared `data_type`: {untyped_columns}"
        )


@check(
    "check_model_has_constraints",
    iterate_over="model",
    params={
        "required_constraint_types": list[str],
    },
)
def check_model_has_constraints(model, ctx, *, required_constraint_types: list[str]):
    """Table and incremental models must have the specified constraint types defined."""
    materialization = (
        model.config.materialized
        if model.config and hasattr(model.config, "materialized")
        else None
    )
    if materialization not in (Materialization.TABLE, Materialization.INCREMENTAL):
        return
    constraints = model.constraints or []
    actual_types: set[str] = set()
    for c in constraints:
        c_type = getattr(c, "type")  # noqa: B009 - avoids ty shadowing of builtin `type`
        actual_types.add(c_type.value if hasattr(c_type, "value") else str(c_type))
    missing_types = sorted(set(required_constraint_types) - actual_types)
    if missing_types:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is missing required constraint types: {missing_types}"
        )
