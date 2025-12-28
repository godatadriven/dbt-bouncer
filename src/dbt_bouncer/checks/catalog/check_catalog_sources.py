from typing import TYPE_CHECKING, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

if TYPE_CHECKING:
    import warnings

    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerSourceBase,
    )

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
            Nodes as CatalogNodes,
        )


class CheckSourceColumnsAreAllDocumented(BaseCheck):
    """All columns in a source should be included in the source's properties file, i.e. `.yml` file.

    Receives:
        catalog_source (CatalogNodes): The CatalogNodes object to check.
        sources (list[DbtBouncerSourceBase]): List of DbtBouncerSourceBase objects parsed from `catalog.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_source_columns_are_all_documented
        ```

    """

    catalog_source: "CatalogNodes | None" = Field(default=None)
    name: Literal["check_source_columns_are_all_documented"]
    sources: list["DbtBouncerSourceBase"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If columns are undocumented.

        """
        if self.catalog_source is None:
            raise DbtBouncerFailedCheckError("self.catalog_source is None")

        source = next(
            s for s in self.sources if s.unique_id == self.catalog_source.unique_id
        )
        undocumented_columns = [
            v.name
            for _, v in self.catalog_source.columns.items()
            if v.name not in source.columns
        ]
        if undocumented_columns:
            raise DbtBouncerFailedCheckError(
                f"`{self.catalog_source.unique_id}` has columns that are not included in the sources properties file: {undocumented_columns}"
            )
