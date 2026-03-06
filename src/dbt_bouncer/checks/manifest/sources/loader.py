"""Checks related to source loaders."""

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerSourceBase,
    )

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class CheckSourceLoaderPopulated(BaseCheck):
    """Sources must have a populated loader.

    Parameters:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_loader_populated
        ```

    """

    name: Literal["check_source_loader_populated"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If loader is not populated.

        """
        self._require_source()
        if self.source.loader == "":
            raise DbtBouncerFailedCheckError(
                f"`{self.source.source_name}.{self.source.name}` does not have a populated loader."
            )
