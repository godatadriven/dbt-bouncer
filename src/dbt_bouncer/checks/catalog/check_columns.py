# mypy: disable-error-code="union-attr"

import re
from typing import TYPE_CHECKING, List, Literal

if TYPE_CHECKING:
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogTable
    from dbt_bouncer.parsers import DbtBouncerModel, DbtBouncerTest

from dbt_bouncer.check_base import BaseCheck


class CheckColumnDescriptionPopulated(BaseCheck):
    name: Literal["check_column_description_populated"]


def check_column_description_populated(
    catalog_node: "CatalogTable",
    models: List["DbtBouncerModel"],
    **kwargs,
) -> None:
    """Columns must have a populated description.

    Parameters:
        catalog_node (CatalogTable): The CatalogTable object to check.
        models (List[DbtBouncerModel]): List of DbtBouncerModel objects parsed from `manifest.json`.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_column_description_populated
              include: ^models/marts
        ```

    """
    if catalog_node.unique_id.split(".")[0] == "model":
        model = next(m for m in models if m.unique_id == catalog_node.unique_id)
        non_complying_columns = []
        for _, v in catalog_node.columns.items():
            if (
                model.columns.get(v.name) is None
                or len(model.columns[v.name].description.strip()) <= 4
            ):
                non_complying_columns.append(v.name)

        assert not non_complying_columns, f"`{catalog_node.unique_id.split('.')[-1]}` has columns that do not have a populated description: {non_complying_columns}"


class CheckColumnNameCompliesToColumnType(BaseCheck):
    column_name_pattern: str
    name: Literal["check_column_name_complies_to_column_type"]
    types: List[str]


def check_column_name_complies_to_column_type(
    catalog_node: "CatalogTable",
    column_name_pattern: str,
    types: List[str],
    **kwargs,
) -> None:
    """Columns with specified data types must comply to the specified regexp naming pattern.

    Parameters:
        catalog_node (CatalogTable): The CatalogTable object to check.
        column_name_pattern: (str): Regex pattern to match the model name.
        types (List[str]): List of data types to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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
        if v.type in types
        and re.compile(column_name_pattern.strip()).match(v.name) is None
    ]

    assert not non_complying_columns, f"`{catalog_node.unique_id.split('.')[-1]}` has columns that don't comply with the specified regexp pattern (`{column_name_pattern}`): {non_complying_columns}"


class CheckColumnsAreAllDocumented(BaseCheck):
    name: Literal["check_columns_are_all_documented"]


def check_columns_are_all_documented(
    catalog_node: "CatalogTable",
    models: List["DbtBouncerModel"],
    **kwargs,
) -> None:
    """All columns in a model should be included in the model's properties file, i.e. `.yml` file.

    Parameters:
        catalog_node (CatalogTable): The CatalogTable object to check.
        models (List[DbtBouncerModel]): List of DbtBouncerModel objects parsed from `manifest.json`.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_columns_are_all_documented
        ```

    """
    if catalog_node.unique_id.split(".")[0] == "model":
        model = next(m for m in models if m.unique_id == catalog_node.unique_id)
        undocumented_columns = [
            v.name
            for _, v in catalog_node.columns.items()
            if v.name not in model.columns
        ]
        assert not undocumented_columns, f"`{catalog_node.unique_id.split('.')[-1]}` has columns that are not included in the models properties file: {undocumented_columns}"


class CheckColumnsAreDocumentedInPublicModels(BaseCheck):
    name: Literal["check_columns_are_documented_in_public_models"]


def check_columns_are_documented_in_public_models(
    catalog_node: "CatalogTable",
    models: List["DbtBouncerModel"],
    **kwargs,
) -> None:
    """Columns should have a populated description in public models.

    Parameters:
        catalog_node (CatalogTable): The CatalogTable object to check.
        models (List[DbtBouncerModel]): List of DbtBouncerModel objects parsed from `manifest.json`.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_columns_are_documented_in_public_models
        ```

    """
    if catalog_node.unique_id.split(".")[0] == "model":
        model = next(m for m in models if m.unique_id == catalog_node.unique_id)
        non_complying_columns = []
        for _, v in catalog_node.columns.items():
            if model.access.value == "public":
                column_config = model.columns.get(v.name)
                if column_config is None or len(column_config.description.strip()) < 4:
                    non_complying_columns.append(v.name)

        assert not non_complying_columns, f"`{catalog_node.unique_id.split('.')[-1]}` is a public model but has columns that don't have a populated description: {non_complying_columns}"


class CheckColumnHasSpecifiedTest(BaseCheck):
    column_name_pattern: str
    name: Literal["check_column_has_specified_test"]
    test_name: str


def check_column_has_specified_test(
    catalog_node: "CatalogTable",
    column_name_pattern: str,
    test_name: str,
    tests: List["DbtBouncerTest"],
    **kwargs,
) -> None:
    """Columns that match the specified regexp pattern must have a specified test.

    Parameters:
        catalog_node (CatalogTable): The CatalogTable object to check.
        column_name_pattern (str): Regex pattern to match the column name.
        test_name (str): Name of the test to check for.
        tests (List[DbtBouncerTest]): List of DbtBouncerTest objects parsed from `manifest.json`.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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
        if t.test_metadata.name == test_name
        and t.attached_node == catalog_node.unique_id
    ]
    non_complying_columns = [
        c
        for c in columns_to_check
        if f"{catalog_node.unique_id}.{c}"
        not in [f"{t.attached_node}.{t.column_name}" for t in relevant_tests]
    ]

    assert not non_complying_columns, f"`{catalog_node.unique_id.split('.')[-1]}` has columns that should have a `{test_name}` test: {non_complying_columns}"
