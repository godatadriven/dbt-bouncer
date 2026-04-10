"""Checks related to source tags."""

from typing import Literal

from dbt_bouncer.check_decorator import check, fail


@check
def check_source_has_tags(
    source, *, criteria: Literal["any", "all", "one"] = "all", tags: list[str]
):
    """Sources must have the specified tags.

    !!! info "Rationale"

        Tags are the primary mechanism for grouping dbt nodes into logical categories — domain areas, sensitivity levels, scheduling tiers, or compliance scopes. When sources are missing required tags, they fall outside automated workflows that rely on tag-based selection (e.g. `dbt build --select tag:pii` or scheduled refreshes filtered by domain). This check ensures that every source is tagged correctly at registration time, preventing ungrouped sources from slipping through governance and operational processes.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the source must have any, all, or exactly one of the specified tags. Default: `all`.
        tags (list[str]): List of tags to check for.

    Receives:
        source (SourceNode): The SourceNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_has_tags
              tags:
                - tag_1
                - tag_2
        ```

    """
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
