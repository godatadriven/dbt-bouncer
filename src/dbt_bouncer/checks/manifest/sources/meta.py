"""Checks related to source meta configuration."""

from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_patterns import BaseHasMetaKeysCheck


class CheckSourceHasMetaKeys(BaseHasMetaKeysCheck):
    """The `meta` config for sources must have the specified keys.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

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
            - name: check_source_has_meta_keys
              keys:
                - contact:
                    - email
                    - slack
                - owner
        ```

    """

    name: Literal["check_source_has_meta_keys"]
    source: Any | None = Field(default=None)

    @property
    def _resource_meta(self) -> dict[str, Any]:
        return self._require_source().meta

    @property
    def _resource_display_name(self) -> str:
        source = self._require_source()
        return f"{source.source_name}.{source.name}"
