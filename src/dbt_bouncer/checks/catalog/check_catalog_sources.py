from typing import TYPE_CHECKING, List, Literal

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.parsers import DbtBouncerCatalogNode, DbtBouncerSource


class CheckSourceColumnsAreAllDocumented(BaseCheck):
    name: Literal["check_source_columns_are_all_documented"]


def check_source_columns_are_all_documented(
    catalog_source: "DbtBouncerCatalogNode",
    sources: List["DbtBouncerSource"],
    **kwargs,
) -> None:
    """All columns in a source should be included in the source's properties file, i.e. `.yml` file.

    Parameters:
        catalog_source (DbtBouncerCatalogNode): The DbtBouncerCatalogNode object to check.
        sources (List[DbtBouncerSource]): List of DbtBouncerSource objects parsed from `catalog.json`.

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
    source = next(s for s in sources if s.unique_id == catalog_source.unique_id)
    undocumented_columns = [
        v.name
        for _, v in catalog_source.columns.items()
        if v.name not in source.columns
    ]
    assert not undocumented_columns, f"`{catalog_source.unique_id}` has columns that are not included in the sources properties file: {undocumented_columns}"
