"""Checks related to source naming conventions."""

from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_patterns import BaseNamePatternCheck


class CheckSourceNames(BaseNamePatternCheck):
    """Sources must have a name that matches the supplied regex.

    Parameters:
        source_name_pattern (str): Regexp the source name must match.

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
            - name: check_source_names
              source_name_pattern: >
                ^[a-z0-9_]*$
        ```

    """

    name: Literal["check_source_names"]
    source: Any | None = Field(default=None)
    source_name_pattern: str

    @property
    def _name_pattern(self) -> str:
        return self.source_name_pattern

    @property
    def _resource_name(self) -> str:
        return str(self._require_source().name)

    @property
    def _resource_display_name(self) -> str:
        source = self._require_source()
        return f"{source.source_name}.{source.name}"
