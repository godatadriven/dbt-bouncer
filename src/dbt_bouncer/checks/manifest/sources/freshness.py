"""Checks related to source freshness."""

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerSourceBase,
    )

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class CheckSourceFreshnessPopulated(BaseCheck):
    """Sources must have a populated freshness.

    Receives:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_freshness_populated
        ```

    """

    name: Literal["check_source_freshness_populated"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If freshness is not populated.

        """
        self._require_source()
        error_msg = f"`{self.source.source_name}.{self.source.name}` does not have a populated freshness."
        if self.source.freshness is None:
            raise DbtBouncerFailedCheckError(error_msg)
        has_error_after = (
            self.source.freshness.error_after
            and self.source.freshness.error_after.count is not None
            and self.source.freshness.error_after.period is not None
        )
        has_warn_after = (
            self.source.freshness.warn_after
            and self.source.freshness.warn_after.count is not None
            and self.source.freshness.warn_after.period is not None
        )

        if not (has_error_after or has_warn_after):
            raise DbtBouncerFailedCheckError(error_msg)
