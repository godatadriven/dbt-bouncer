"""Checks related to source tags."""

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.enums import Criteria


@check(code="SO015")
def check_source_has_tags(
    source, *, criteria: Criteria = Criteria.ALL, tags: list[str]
):
    """Sources must have the specified tags.

    !!! info "Rationale"

        Tags are the primary mechanism for grouping dbt nodes into logical categories — domain areas, sensitivity levels, scheduling tiers, or compliance scopes. When sources are missing required tags, they fall outside automated workflows that rely on tag-based selection (e.g. `dbt build --select tag:pii` or scheduled refreshes filtered by domain). This check ensures that every source is tagged correctly at registration time, preventing ungrouped sources from slipping through governance and operational processes.

    Parameters:
        criteria (Literal["all", "any", "one"]): Whether the source must have any, all, or exactly one of the specified tags. Default: `all`.
        tags (list[str]): List of tags to check for.

    Receives:
        source (SourceNode): The SourceNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Source paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Only source paths that match any pattern will be checked.
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

    if criteria == Criteria.ANY:
        if not any(tag in resource_tags for tag in tags):
            fail(f"`{display}` does not have any of the required tags: {tags}.")
    elif criteria == Criteria.ALL:
        missing_tags = [tag for tag in tags if tag not in resource_tags]
        if missing_tags:
            fail(f"`{display}` is missing required tags: {missing_tags}.")
    elif criteria == Criteria.ONE and sum(tag in resource_tags for tag in tags) != 1:
        fail(f"`{display}` must have exactly one of the required tags: {tags}.")
