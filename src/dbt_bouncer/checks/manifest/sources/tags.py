"""Checks related to source tags."""

from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_patterns import BaseHasTagsCheck


class CheckSourceHasTags(BaseHasTagsCheck):
    """Sources must have the specified tags.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the source must have any, all, or exactly one of the specified tags. Default: `all`.
        source (SourceNode): The SourceNode object to check.
        tags (list[str]): List of tags to check for.

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

    name: Literal["check_source_has_tags"]
    source: Any | None = Field(default=None)

    @property
    def _resource_tags(self) -> list[str]:
        return self._require_source().tags or []

    @property
    def _resource_display_name(self) -> str:
        source = self._require_source()
        return f"{source.source_name}.{source.name}"
