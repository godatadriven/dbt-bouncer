from typing import Literal

import pytest

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.utils import get_check_inputs


class CheckColumnDataMustEndUnderscoreDate(BaseCheck):
    name: Literal["check_column_data_must_end_underscore_date"]


@pytest.mark.iterate_over_catalog_nodes
def check_column_data_must_end_underscore_date(
    request, check_config=None, catalog_node=None
) -> None:
    """
    Columns with the type "DATE" must end with "_date".
    """

    catalog_node = get_check_inputs(catalog_node=catalog_node, request=request)["catalog_node"]

    date_columns_without_postfix = [
        v["name"]
        for k, v in catalog_node["columns"].items()
        if v["type"] == "DATE" and not v["name"].endswith("_date")
    ]

    assert (
        not date_columns_without_postfix
    ), f"`{catalog_node['unique_id'].split('.')[-1]}` has columns with `DATE` type must end with `_date`: {date_columns_without_postfix}"
