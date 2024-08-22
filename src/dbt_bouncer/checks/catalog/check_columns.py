# mypy: disable-error-code="union-attr"

import re
import warnings
from typing import List, Literal, Union

import pytest
from _pytest.fixtures import TopRequest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
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

    Receives:
        catalog_node (CatalogTable): The CatalogTable object to check.
        column_name_pattern: (str): Regex pattern to match the model name.
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.

    Example(s):
        ```yaml
        catalog_checks:
            # DATE columns must end with "_date"
            - name: check_column_name_complies_to_column_type
              column_name_pattern: .*_date$
              types:
                - DATE
        ```
        ```yaml
        catalog_checks:
            # BOOLEAN columns must start with "is_"
            - name: check_column_name_complies_to_column_type
              column_name_pattern: ^is_.*
              types:
                - BOOLEAN
        ```
        ```yaml
        catalog_checks:
            # Columns of all types must consist of lowercase letters and underscores. Note that the specified types depend on the underlying database.
            - name: check_column_name_complies_to_column_type
              column_name_pattern: ^[a-z_]*$
              types:
                - BIGINT
                - BOOLEAN
                - DATE
                - DOUBLE
                - INTEGER
                - VARCHAR
        ```
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

    Receives:
        catalog_node (CatalogTable): The CatalogTable object to check.
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_columns_are_all_documented
        ```
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

    Receives:
        catalog_node (CatalogTable): The CatalogTable object to check.
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_columns_are_documented_in_public_models
        ```
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

    Receives:
        catalog_node (CatalogTable): The CatalogTable object to check.
        column_name_pattern (str): Regex pattern to match the column name.
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        test_name (str): Name of the test to check for.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_column_has_specified_test
              column_name_pattern: ^is_.*
              test_name: not_null
        ```
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
