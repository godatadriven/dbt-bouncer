"""Checks related to source descriptions."""

from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_patterns import BaseDescriptionPopulatedCheck


class CheckSourceDescriptionPopulated(BaseDescriptionPopulatedCheck):
    """Sources must have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

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
            - name: check_source_description_populated
        ```
        ```yaml
        manifest_checks:
            - name: check_source_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """

    name: Literal["check_source_description_populated"]
    source: Any | None = Field(default=None)

    @property
    def _resource_description(self) -> str:
        return self._require_source().description or ""

    @property
    def _resource_display_name(self) -> str:
        source = self._require_source()
        return f"{source.source_name}.{source.name}"
