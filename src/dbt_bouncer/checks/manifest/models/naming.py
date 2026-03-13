"""Checks related to model naming conventions."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


@check(
    "check_model_names",
    iterate_over="model",
    params={"model_name_pattern": str},
)
def check_model_names(model, ctx, *, model_name_pattern: str):
    """Models must have a name that matches the supplied regex."""
    compiled = compile_pattern(model_name_pattern.strip())
    if compiled.match(str(model.name)) is None:
        display_name = get_clean_model_name(model.unique_id)
        fail(
            f"`{display_name}` does not match the supplied regex `{model_name_pattern.strip()}`."
        )
