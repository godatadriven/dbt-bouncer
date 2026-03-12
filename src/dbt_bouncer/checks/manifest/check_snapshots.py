from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_patterns import BaseHasTagsCheck, BaseNamePatternCheck


class CheckSnapshotHasTags(BaseHasTagsCheck):
    """Snapshots must have the specified tags.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the snapshot must have any, all, or exactly one of the specified tags. Default: `all`.
        snapshot (SnapshotNode): The SnapshotNode object to check.
        tags (list[str]): List of tags to check for.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the snapshot path. Snapshot paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the snapshot path. Only snapshot paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_snapshot_has_tags
              tags:
                - tag_1
                - tag_2
        ```

    """

    name: Literal["check_snapshot_has_tags"]
    snapshot: Any | None = Field(default=None)

    @property
    def _resource_tags(self) -> list[str]:
        return self._require_snapshot().tags or []

    @property
    def _resource_display_name(self) -> str:
        return str(self._require_snapshot().name)


class CheckSnapshotNames(BaseNamePatternCheck):
    """Snapshots must have a name that matches the supplied regex.

    Parameters:
        snapshot_name_pattern (str): Regexp the snapshot name must match.

    Receives:
        snapshot (SnapshotNode): The SnapshotNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the snapshot path. Snapshot paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the snapshot path. Only snapshot paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_snapshot_names
              include: ^snapshots/erp
              snapshot_name_pattern: ^erp
        ```

    """

    name: Literal["check_snapshot_names"]
    snapshot: Any | None = Field(default=None)
    snapshot_name_pattern: str

    @property
    def _name_pattern(self) -> str:
        return self.snapshot_name_pattern

    @property
    def _resource_name(self) -> str:
        return str(self._require_snapshot().name)

    @property
    def _resource_display_name(self) -> str:
        return str(self._require_snapshot().name)
