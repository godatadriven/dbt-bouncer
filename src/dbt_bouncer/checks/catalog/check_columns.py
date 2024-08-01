import re
from typing import List, Literal

import pytest

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.utils import get_check_inputs


class CheckColumnNameCompliesToColumnType(BaseCheck):
    column_name_pattern: str
    name: Literal["check_column_name_complies_to_column_type"]
    types: List[str]


@pytest.mark.iterate_over_catalog_nodes
def check_column_name_complies_to_column_type(
    request, check_config=None, catalog_node=None
) -> None:
    """
    Columns with specified data types must comply to the specified regexp naming pattern.
    """

    input_vars = get_check_inputs(
        catalog_node=catalog_node, check_config=check_config, request=request
    )
    catalog_node = input_vars["catalog_node"]
    check_config = input_vars["check_config"]

    non_complying_columns = [
        v["name"]
        for k, v in catalog_node["columns"].items()
        if v["type"] in check_config["types"]
        and re.compile(check_config["column_name_pattern"].strip()).match(v["name"]) is None
    ]

    assert (
        not non_complying_columns
    ), f"""`{catalog_node['unique_id'].split('.')[-1]}` has columns that don't comply with the specified regexp pattern (`{check_config['column_name_pattern']}`): {non_complying_columns}"""
