# mypy: disable-error-code="union-attr"

import re
from typing import List, Literal, Union

import pytest
from _pytest.fixtures import TopRequest
from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogTable

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.parsers import DbtBouncerModel, DbtBouncerTest
from dbt_bouncer.utils import bouncer_check


class CheckColumnNameCompliesToColumnType(BaseCheck):
    column_name_pattern: str
    name: Literal["check_column_name_complies_to_column_type"]
    types: List[str]


@pytest.mark.iterate_over_catalog_nodes
@bouncer_check
def check_column_name_complies_to_column_type(
    request: TopRequest,
    catalog_node: Union[CatalogTable, None] = None,
    column_name_pattern: Union[None, str] = None,
    types: Union[List[str], None] = None,
    **kwargs,
) -> None:
    """
    Columns with specified data types must comply to the specified regexp naming pattern.
    """

    non_complying_columns = [
        v.name
        for _, v in catalog_node.columns.items()
        if v.type in types and re.compile(column_name_pattern.strip()).match(v.name) is None  # type: ignore[operator]
    ]

    assert (
        not non_complying_columns
    ), f"`{catalog_node.unique_id.split('.')[-1]}` has columns that don't comply with the specified regexp pattern (`{column_name_pattern}`): {non_complying_columns}"


class CheckColumnsAreAllDocumented(BaseCheck):
    name: Literal["check_columns_are_all_documented"]


@pytest.mark.iterate_over_catalog_nodes
@bouncer_check
def check_columns_are_all_documented(
    models: List[DbtBouncerModel],
    request: TopRequest,
    catalog_node: Union[CatalogTable, None] = None,
    **kwargs,
) -> None:
    """
    All columns in a model should be included in the model's properties file, i.e. `.yml` file.
    """

    if catalog_node.unique_id.split(".")[0] == "model":
        model = [m for m in models if m.unique_id == catalog_node.unique_id][0]
        undocumented_columns = [
            v.name for _, v in catalog_node.columns.items() if v.name not in model.columns.keys()
        ]
        assert (
            not undocumented_columns
        ), f"`{catalog_node.unique_id.split('.')[-1]}` has columns that are not included in the models properties file: {undocumented_columns}"


class CheckColumnsAreDocumentedInPublicModels(BaseCheck):
    name: Literal["check_columns_are_documented_in_public_models"]


@pytest.mark.iterate_over_catalog_nodes
@bouncer_check
def check_columns_are_documented_in_public_models(
    models: List[DbtBouncerModel],
    request: TopRequest,
    catalog_node: Union[CatalogTable, None] = None,
    **kwargs,
) -> None:
    """
    Columns should have a populated description in public models.
    """

    if catalog_node.unique_id.split(".")[0] == "model":
        model = [m for m in models if m.unique_id == catalog_node.unique_id][0]
        non_complying_columns = []
        for k, v in catalog_node.columns.items():
            if model.access.value == "public":
                column_config = model.columns.get(v.name)
                if column_config is None or len(column_config.description.strip()) < 4:
                    non_complying_columns.append(v.name)

        assert (
            not non_complying_columns
        ), f"`{catalog_node.unique_id.split('.')[-1]}` is a public model but has columns that don't have a populated description: {non_complying_columns}"


class CheckColumnHasSpecifiedTest(BaseCheck):
    column_name_pattern: str
    name: Literal["check_column_has_specified_test"]
    test_name: str


@pytest.mark.iterate_over_catalog_nodes
@bouncer_check
def check_column_has_specified_test(
    request: TopRequest,
    tests: List[DbtBouncerTest],
    catalog_node: Union[CatalogTable, None] = None,
    column_name_pattern: Union[None, str] = None,
    test_name: Union[None, str] = None,
    **kwargs,
) -> None:
    """
    Columns that match the specified regexp pattern must have a specified test.
    """

    columns_to_check = [
        v.name
        for _, v in catalog_node.columns.items()
        if re.compile(column_name_pattern.strip()).match(v.name) is not None
    ]
    relevant_tests = [
        t
        for t in tests
        if t.test_metadata.name == test_name and t.attached_node == catalog_node.unique_id
    ]
    non_complying_columns = [
        c
        for c in columns_to_check
        if f"{catalog_node.unique_id}.{c}"
        not in [f"{t.attached_node}.{t.column_name}" for t in relevant_tests]
    ]

    assert (
        not non_complying_columns
    ), f"`{catalog_node.unique_id.split('.')[-1]}` has columns that should have a `{test_name}` test: {non_complying_columns}"
