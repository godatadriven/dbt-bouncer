import re
from typing import TYPE_CHECKING, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerSnapshotBase,
    )

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class CheckSnapshotHasTags(BaseCheck):
    """Snapshots must have the specified tags.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the snapshot must have any, all, or exactly one of the specified tags. Default: `all`.
        snapshot (DbtBouncerSnapshotBase): The DbtBouncerSnapshotBase object to check.
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

    criteria: Literal["any", "all", "one"] = Field(default="all")
    name: Literal["check_snapshot_has_tags"]
    snapshot: "DbtBouncerSnapshotBase | None" = Field(default=None)
    tags: list[str]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If snapshot does not have required tags.

        """
        if self.snapshot is None:
            raise DbtBouncerFailedCheckError("self.snapshot is None")
        snapshot_tags = self.snapshot.tags or []
        if self.criteria == "any":
            if not any(tag in snapshot_tags for tag in self.tags):
                raise DbtBouncerFailedCheckError(
                    f"`{self.snapshot.name}` does not have any of the required tags: {self.tags}."
                )
        elif self.criteria == "all":
            missing_tags = [tag for tag in self.tags if tag not in snapshot_tags]
            if missing_tags:
                raise DbtBouncerFailedCheckError(
                    f"`{self.snapshot.name}` is missing required tags: {missing_tags}."
                )
        elif (
            self.criteria == "one"
            and sum(tag in snapshot_tags for tag in self.tags) != 1
        ):
            raise DbtBouncerFailedCheckError(
                f"`{self.snapshot.name}` must have exactly one of the required tags: {self.tags}."
            )


class CheckSnapshotNames(BaseCheck):
    """Snapshots must have a name that matches the supplied regex.

    Parameters:
        snapshot_name_pattern (str): Regexp the snapshot name must match.

    Receives:
        snapshot (DbtBouncerSnapshotBase): The DbtBouncerSnapshotBase object to check.

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
    snapshot: "DbtBouncerSnapshotBase | None" = Field(default=None)
    snapshot_name_pattern: str

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If snapshot name does not match regex.

        """
        if self.snapshot is None:
            raise DbtBouncerFailedCheckError("self.snapshot is None")
        if (
            re.compile(self.snapshot_name_pattern.strip()).match(
                str(self.snapshot.name)
            )
            is None
        ):
            raise DbtBouncerFailedCheckError(
                f"`{self.snapshot.name}` does not match the supplied regex `{self.snapshot_name_pattern.strip()})`."
            )
