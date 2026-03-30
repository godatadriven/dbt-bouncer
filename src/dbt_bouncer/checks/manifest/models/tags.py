"""Checks related to model tags."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import get_clean_model_name


@check
def check_model_has_tags(model, *, criteria: str = "all", tags: list[str]):
    """Models must have the specified tags.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the model must have any, all, or exactly one of the specified tags. Default: `any`.
        tags (list[str]): List of tags to check for.

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
            - name: check_model_has_tags
              tags:
                - tag_1
                - tag_2
        ```

    """
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
