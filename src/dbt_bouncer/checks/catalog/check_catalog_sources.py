from typing import TYPE_CHECKING, List, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    import warnings

    from dbt_bouncer.parsers import (
        DbtBouncerSourceBase,
    )

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogTable


class CheckSourceColumnsAreAllDocumented(BaseCheck):
    """All columns in a source should be included in the source's properties file, i.e. `.yml` file.

    Receives:
        catalog_source (CatalogTable): The CatalogTable object to check.
        sources (List[DbtBouncerSourceBase]): List of DbtBouncerSourceBase objects parsed from `catalog.json`.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_source_columns_are_all_documented
        ```

    """

    catalog_source: "CatalogTable" = Field(default=None)
    name: Literal["check_source_columns_are_all_documented"]
    sources: List["DbtBouncerSourceBase"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check."""
        source = next(
            s for s in self.sources if s.unique_id == self.catalog_source.unique_id
        )
        undocumented_columns = [
            v.name
            for _, v in self.catalog_source.columns.items()
            if v.name not in source.columns
        ]
        assert not undocumented_columns, f"`{self.catalog_source.unique_id}` has columns that are not included in the sources properties file: {undocumented_columns}"
