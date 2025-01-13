# mypy: disable-error-code="union-attr"

import re
from typing import TYPE_CHECKING, List, Literal

if TYPE_CHECKING:
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
            Nodes as CatalogNodes,
        )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
        DbtBouncerTestBase,
    )

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck


class CheckColumnDescriptionPopulated(BaseCheck):
    """Columns must have a populated description.

    Receives:
        catalog_node (CatalogNodes): The CatalogNodes object to check.
        models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

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

    catalog_node: "CatalogNodes" = Field(default=None)
    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_column_description_populated"]

    def execute(self) -> None:
        """Execute the check."""
        if self.catalog_node.unique_id.split(".")[0] == "model":
            model = next(
                m for m in self.models if m.unique_id == self.catalog_node.unique_id
            )
            non_complying_columns = []
            for _, v in self.catalog_node.columns.items():
                if (
                    model.columns.get(v.name) is None
                    or len(model.columns[v.name].description.strip()) <= 4
                ):
                    non_complying_columns.append(v.name)

            assert not non_complying_columns, (
                f"`{self.catalog_node.unique_id.split('.')[-1]}` has columns that do not have a populated description: {non_complying_columns}"
            )


class CheckColumnHasSpecifiedTest(BaseCheck):
    """Columns that match the specified regexp pattern must have a specified test.

    Parameters:
        column_name_pattern (str): Regex pattern to match the column name.
        test_name (str): Name of the test to check for.

    Receives:
        catalog_node (CatalogNodes): The CatalogNodes object to check.
        tests (List[DbtBouncerTestBase]): List of DbtBouncerTestBase objects parsed from `manifest.json`.

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

    catalog_node: "CatalogNodes" = Field(default=None)
    column_name_pattern: str
    name: Literal["check_column_has_specified_test"]
    test_name: str
    tests: List["DbtBouncerTestBase"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check."""
        columns_to_check = [
            v.name
            for _, v in self.catalog_node.columns.items()
            if re.compile(self.column_name_pattern.strip()).match(v.name) is not None
        ]
        relevant_tests = [
            t
            for t in self.tests
            if hasattr(t, "test_metadata") is True
            and hasattr(t, "attached_node") is True
            and t.test_metadata.name == self.test_name
            and t.attached_node == self.catalog_node.unique_id
        ]
        non_complying_columns = [
            c
            for c in columns_to_check
            if f"{self.catalog_node.unique_id}.{c}"
            not in [f"{t.attached_node}.{t.column_name}" for t in relevant_tests]
        ]

        assert not non_complying_columns, (
            f"`{self.catalog_node.unique_id.split('.')[-1]}` has columns that should have a `{self.test_name}` test: {non_complying_columns}"
        )


class CheckColumnNameCompliesToColumnType(BaseCheck):
    """Columns with specified data types must comply to the specified regexp naming pattern.

    Parameters:
        column_name_pattern (str): Regex pattern to match the model name.
        types (List[str]): List of data types to check.

    Receives:
        catalog_node (CatalogNodes): The CatalogNodes object to check.

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

    catalog_node: "CatalogNodes" = Field(default=None)
    column_name_pattern: str
    name: Literal["check_column_name_complies_to_column_type"]
    types: List[str]

    def execute(self) -> None:
        """Execute the check."""
        non_complying_columns = [
            v.name
            for _, v in self.catalog_node.columns.items()
            if v.type in self.types
            and re.compile(self.column_name_pattern.strip()).match(v.name) is None
        ]

        assert not non_complying_columns, (
            f"`{self.catalog_node.unique_id.split('.')[-1]}` has columns that don't comply with the specified regexp pattern (`{self.column_name_pattern}`): {non_complying_columns}"
        )


class CheckColumnsAreAllDocumented(BaseCheck):
    """All columns in a model should be included in the model's properties file, i.e. `.yml` file.

    Receives:
        catalog_node (CatalogNodes): The CatalogNodes object to check.
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

    catalog_node: "CatalogNodes" = Field(default=None)
    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_columns_are_all_documented"]

    def execute(self) -> None:
        """Execute the check."""
        if self.catalog_node.unique_id.split(".")[0] == "model":
            model = next(
                m for m in self.models if m.unique_id == self.catalog_node.unique_id
            )
            undocumented_columns = [
                v.name
                for _, v in self.catalog_node.columns.items()
                if v.name not in model.columns
            ]
            assert not undocumented_columns, (
                f"`{self.catalog_node.unique_id.split('.')[-1]}` has columns that are not included in the models properties file: {undocumented_columns}"
            )


class CheckColumnsAreDocumentedInPublicModels(BaseCheck):
    """Columns should have a populated description in public models.

    Receives:
        catalog_node (CatalogNodes): The CatalogNodes object to check.
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

    catalog_node: "CatalogNodes" = Field(default=None)
    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_columns_are_documented_in_public_models"]

    def execute(self) -> None:
        """Execute the check."""
        if self.catalog_node.unique_id.split(".")[0] == "model":
            model = next(
                m for m in self.models if m.unique_id == self.catalog_node.unique_id
            )
            non_complying_columns = []
            for _, v in self.catalog_node.columns.items():
                if model.access.value == "public":
                    column_config = model.columns.get(v.name)
                    if (
                        column_config is None
                        or len(column_config.description.strip()) < 4
                    ):
                        non_complying_columns.append(v.name)

            assert not non_complying_columns, (
                f"`{self.catalog_node.unique_id.split('.')[-1]}` is a public model but has columns that don't have a populated description: {non_complying_columns}"
            )
