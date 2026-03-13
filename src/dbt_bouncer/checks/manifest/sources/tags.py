"""Checks related to source tags."""

from typing import Literal

from dbt_bouncer.check_decorator import check, fail


@check
def check_source_has_tags(
    source, *, criteria: Literal["any", "all", "one"] = "all", tags: list[str]
):
    """Sources must have the specified tags."""
    resource_tags = source.tags or []
    display = f"{source.source_name}.{source.name}"

    if criteria == "any":
        if not any(tag in resource_tags for tag in tags):
            fail(f"`{display}` does not have any of the required tags: {tags}.")
    elif criteria == "all":
        missing_tags = [tag for tag in tags if tag not in resource_tags]
        if missing_tags:
            fail(f"`{display}` is missing required tags: {missing_tags}.")
    elif criteria == "one" and sum(tag in resource_tags for tag in tags) != 1:
        fail(f"`{display}` must have exactly one of the required tags: {tags}.")
