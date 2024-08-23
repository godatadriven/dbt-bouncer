# mypy: disable-error-code="union-attr"

from typing import List, Literal, Union

import pytest
from _pytest.fixtures import TopRequest

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.parsers import DbtBouncerCatalogNode, DbtBouncerSource
from dbt_bouncer.utils import bouncer_check


class CheckSourceColumnsAreAllDocumented(BaseCheck):
    name: Literal["check_source_columns_are_all_documented"]


@pytest.mark.iterate_over_catalog_sources
@bouncer_check
def check_source_columns_are_all_documented(
    sources: List[DbtBouncerSource],
    request: TopRequest,
    catalog_source: Union[DbtBouncerCatalogNode, None] = None,
    **kwargs,
) -> None:
    """
    All columns in a source should be included in the source's properties file, i.e. `.yml` file.

    Receives:
        catalog_source (DbtBouncerCatalogNode): The DbtBouncerCatalogNode object to check.
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_source_columns_are_all_documented
        ```
    """

    source = [s for s in sources if s.unique_id == catalog_source.unique_id][0]
    undocumented_columns = [
        v.name for _, v in catalog_source.columns.items() if v.name not in source.columns.keys()
    ]
    assert (
        not undocumented_columns
    ), f"`{catalog_source.unique_id}` has columns that are not included in the sources properties file: {undocumented_columns}"
