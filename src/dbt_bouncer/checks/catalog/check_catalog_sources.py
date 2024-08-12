from typing import Literal

import pytest

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.utils import get_check_inputs


class CheckSourceColumnsAreAllDocumented(BaseCheck):
    name: Literal["check_source_columns_are_all_documented"]


@pytest.mark.iterate_over_catalog_sources
def check_source_columns_are_all_documented(sources, request, catalog_source=None) -> None:
    """
    All columns in a source should be included in the source's properties file, i.e. `.yml` file.
    """

    catalog_source = get_check_inputs(catalog_source=catalog_source, request=request)[
        "catalog_source"
    ]

    source = [s for s in sources if s["unique_id"] == catalog_source["unique_id"]][0]
    undocumented_columns = [
        v["name"]
        for _, v in catalog_source["columns"].items()
        if v["name"] not in source["columns"].keys()
    ]
    assert (
        not undocumented_columns
    ), f"`{catalog_source['unique_id']}` has columns that are not included in the sources properties file: {undocumented_columns}"
