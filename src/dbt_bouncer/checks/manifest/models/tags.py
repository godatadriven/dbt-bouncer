"""Checks related to model tags."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import get_clean_model_name


@check(
    "check_model_has_tags",
    iterate_over="model",
    params={"criteria": (str, "all"), "tags": list[str]},
)
def check_model_has_tags(model, ctx, *, criteria: str, tags: list[str]):
    """Models must have the specified tags."""
    resource_tags = model.tags or []
    display_name = get_clean_model_name(model.unique_id)
    if criteria == "any":
        if not any(tag in resource_tags for tag in tags):
            fail(f"`{display_name}` does not have any of the required tags: {tags}.")
    elif criteria == "all":
        missing_tags = [tag for tag in tags if tag not in resource_tags]
        if missing_tags:
            fail(f"`{display_name}` is missing required tags: {missing_tags}.")
    elif criteria == "one" and sum(tag in resource_tags for tag in tags) != 1:
        fail(f"`{display_name}` must have exactly one of the required tags: {tags}.")
