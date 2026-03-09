"""Checks related to source tags."""

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerSourceBase,
    )

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class CheckSourceHasTags(BaseCheck):
    """Sources must have the specified tags.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the source must have any, all, or exactly one of the specified tags. Default: `all`.
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.
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

    criteria: Literal["any", "all", "one"] = Field(default="all")
    name: Literal["check_source_has_tags"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)
    tags: list[str]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If source does not have required tags.

        """
        source = self._require_source()
        source_tags = source.tags or []
        if self.criteria == "any":
            if not any(tag in source_tags for tag in self.tags):
                raise DbtBouncerFailedCheckError(
                    f"`{source.source_name}.{source.name}` does not have any of the required tags: {self.tags}."
                )
        elif self.criteria == "all":
            missing_tags = set(self.tags) - set(source_tags)
            if missing_tags:
                raise DbtBouncerFailedCheckError(
                    f"`{source.source_name}.{source.name}` is missing required tags: {missing_tags}."
                )
        elif (
            self.criteria == "one" and sum(tag in source_tags for tag in self.tags) != 1
        ):
            raise DbtBouncerFailedCheckError(
                f"`{source.source_name}.{source.name}` must have exactly one of the required tags: {self.tags}."
            )
