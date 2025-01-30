# mypy: disable-error-code="union-attr"
import re
from typing import TYPE_CHECKING, List, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_common import (
        DbtBouncerSnapshotBase,
    )


class CheckSnapshotHasTags(BaseCheck):
    """Snapshots must have the specified tags.

    Parameters:
        snapshot (DbtBouncerSnapshotBase): The DbtBouncerSnapshotBase object to check.
        tags (List[str]): List of tags to check for.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the snapshot path. Snapshot paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the snapshot path. Only snapshot paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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
    snapshot: "DbtBouncerSnapshotBase" = Field(default=None)
    tags: List[str]

    def execute(self) -> None:
        """Execute the check."""
        missing_tags = [tag for tag in self.tags if tag not in self.snapshot.tags]
        assert not missing_tags, (
            f"`{self.snapshot.name}` is missing required tags: {missing_tags}."
        )


class CheckSnapshotNames(BaseCheck):
    """Snapshots must have a name that matches the supplied regex.

    Parameters:
        snapshot_name_pattern (str): Regexp the snapshot name must match.

    Receives:
        snapshot (DbtBouncerSnapshotBase): The DbtBouncerSnapshotBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the snapshot path. Snapshot paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the snapshot path. Only snapshot paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_snapshot_names
              include: ^snapshots/erp
              snapshot_name_pattern: ^erp
        ```

    """

    name: Literal["check_snapshot_names"]
    snapshot: "DbtBouncerSnapshotBase" = Field(default=None)
    snapshot_name_pattern: str

    def execute(self) -> None:
        """Execute the check."""
        assert (
            re.compile(self.snapshot_name_pattern.strip()).match(self.snapshot.name)
            is not None
        ), (
            f"`{self.snapshot.name}` does not match the supplied regex `{self.snapshot_name_pattern.strip()})`."
        )
