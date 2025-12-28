import re
from typing import TYPE_CHECKING, Literal

from pydantic import ConfigDict, Field

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

if TYPE_CHECKING:
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
            Nodes as CatalogNodes,
        )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        DbtBouncerModelBase,
        DbtBouncerTestBase,
    )
from pydantic import model_validator

from dbt_bouncer.check_base import BaseCheck


class CheckColumnDescriptionPopulated(BaseCheck):
    """Columns must have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

    Receives:
        catalog_node (CatalogNodes): The CatalogNodes object to check.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_column_description_populated
              include: ^models/marts
        ```
        ```yaml
        manifest_checks:
            - name: check_column_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """

    catalog_node: "CatalogNodes | None" = Field(default=None)
    min_description_length: int | None = Field(default=None)
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_column_description_populated"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If description is not populated.

        """
        if self.catalog_node is None:
            raise DbtBouncerFailedCheckError("self.catalog_node is None")
        if self.is_catalog_node_a_model(self.catalog_node, self.models):
            model = next(
                m for m in self.models if m.unique_id == self.catalog_node.unique_id
            )
            non_complying_columns = []
            for _, v in self.catalog_node.columns.items():
                columns = model.columns or {}
                if columns.get(v.name) is None or not self._is_description_populated(
                    columns[v.name].description or "", self.min_description_length
                ):
                    non_complying_columns.append(v.name)

            if non_complying_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(self.catalog_node.unique_id).split('.')[-1]}` has columns that do not have a populated description: {non_complying_columns}"
                )


class CheckColumnHasSpecifiedTest(BaseCheck):
    """Columns that match the specified regexp pattern must have a specified test.

    Parameters:
        column_name_pattern (str): Regex pattern to match the column name.
        test_name (str): Name of the test to check for.

    Receives:
        catalog_node (CatalogNodes): The CatalogNodes object to check.
        tests (list[DbtBouncerTestBase]): List of DbtBouncerTestBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_column_has_specified_test
              column_name_pattern: ^is_.*
              test_name: not_null
        ```

    """

    catalog_node: "CatalogNodes | None" = Field(default=None)
    column_name_pattern: str
    name: Literal["check_column_has_specified_test"]
    test_name: str
    tests: list["DbtBouncerTestBase"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If column does not have specified test.

        """
        if self.catalog_node is None:
            raise DbtBouncerFailedCheckError("self.catalog_node is None")
        columns_to_check = [
            v.name
            for _, v in self.catalog_node.columns.items()
            if re.compile(self.column_name_pattern.strip()).match(str(v.name))
            is not None
        ]
        relevant_tests = []
        for t in self.tests:
            test_metadata = getattr(t, "test_metadata", None)
            attached_node = getattr(t, "attached_node", None)
            if (
                test_metadata
                and attached_node
                and getattr(test_metadata, "name", None) == self.test_name
                and attached_node == self.catalog_node.unique_id
            ):
                relevant_tests.append(t)
        non_complying_columns = [
            c
            for c in columns_to_check
            if f"{self.catalog_node.unique_id}.{c}"
            not in [
                f"{getattr(t, 'attached_node', '')}.{getattr(t, 'column_name', '')}"
                for t in relevant_tests
            ]
        ]

        if non_complying_columns:
            raise DbtBouncerFailedCheckError(
                f"`{str(self.catalog_node.unique_id).split('.')[-1]}` has columns that should have a `{self.test_name}` test: {non_complying_columns}"
            )


class CheckColumnNameCompliesToColumnType(BaseCheck):
    """Columns with the specified regexp naming pattern must have data types that comply to the specified regexp pattern or list of data types.

    Note: One of `type_pattern` or `types` must be specified.

    Parameters:
        column_name_pattern (str): Regex pattern to match the model name.
        type_pattern (str | None): Regex pattern to match the data types.
        types (list[str] | None): List of data types to check.

    Receives:
        catalog_node (CatalogNodes): The CatalogNodes object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

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
        ```yaml
        catalog_checks:
            # No STRUCT data types permitted.
            - name: check_column_name_complies_to_column_type
              column_name_pattern: ^[a-z_]*$
              type_pattern: ^(?!STRUCT)
        ```

    """

    catalog_node: "CatalogNodes | None" = Field(default=None)
    column_name_pattern: str
    name: Literal["check_column_name_complies_to_column_type"]
    type_pattern: str | None = None
    types: list[str] | None = None

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If column name does not comply to column type.

        """
        if self.catalog_node is None:
            raise DbtBouncerFailedCheckError("self.catalog_node is None")
        if self.type_pattern:
            non_complying_columns = [
                v.name
                for _, v in self.catalog_node.columns.items()
                if re.compile(self.type_pattern.strip()).match(str(v.type)) is None
                and re.compile(self.column_name_pattern.strip()).match(str(v.name))
                is not None
            ]

            if non_complying_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(self.catalog_node.unique_id).split('.')[-1]}` has columns that don't comply with the specified data type regexp pattern (`{self.column_name_pattern}`): {non_complying_columns}"
                )

        elif self.types:
            non_complying_columns = [
                v.name
                for _, v in self.catalog_node.columns.items()
                if v.type in self.types
                and re.compile(self.column_name_pattern.strip()).match(str(v.name))
                is None
            ]

            if non_complying_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(self.catalog_node.unique_id).split('.')[-1]}` has columns that don't comply with the specified regexp pattern (`{self.column_name_pattern}`): {non_complying_columns}"
                )

    @model_validator(mode="after")
    def _check_type_pattern_or_types(self) -> "CheckColumnNameCompliesToColumnType":
        if not (self.type_pattern or self.types):
            raise ValueError("Either 'type_pattern' or 'types' must be supplied.")
        if self.type_pattern is not None and self.types is not None:
            raise ValueError("Only one of 'type_pattern' or 'types' can be supplied.")
        return self


class CheckColumnNames(BaseCheck):
    """Columns must have a name that matches the supplied regex.

    Parameters:
        columns_name_pattern (str): Regexp the column name must match.

    Receives:
        catalog_node (CatalogNodes): The CatalogNodes object to check.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_column_names
              column_name_pattern: [a-z_] # Lowercase only, underscores allowed
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    catalog_node: "CatalogNodes | None" = Field(default=None)
    column_name_pattern: str
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_column_names"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If column name does not match regex.

        """
        if self.catalog_node is None:
            raise DbtBouncerFailedCheckError("self.catalog_node is None")
        if self.is_catalog_node_a_model(self.catalog_node, self.models):
            non_complying_columns: list[str] = []
            non_complying_columns.extend(
                v.name
                for _, v in self.catalog_node.columns.items()
                if re.fullmatch(self.column_name_pattern.strip(), str(v.name)) is None
            )

            if non_complying_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(self.catalog_node.unique_id).split('.')[-1]}` has columns ({non_complying_columns}) that do not match the supplied regex: `{self.column_name_pattern.strip()}`."
                )


class CheckColumnsAreAllDocumented(BaseCheck):
    """All columns in a model should be included in the model's properties file, i.e. `.yml` file.

    Receives:
        case_sensitive (bool | None): Whether the column names are case sensitive or not. Necessary for adapters like `dbt-snowflake` where the column in `catalog.json` is uppercase but the column in `manifest.json` can be lowercase. Defaults to `false` for `dbt-snowflake`, otherwise `true`.
        catalog_node (CatalogNodes): The CatalogNodes object to check.
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_columns_are_all_documented
        ```

    """

    case_sensitive: bool | None = Field(default=True)
    catalog_node: "CatalogNodes | None" = Field(default=None)
    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_columns_are_all_documented"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If columns are undocumented.

        """
        if self.catalog_node is None:
            raise DbtBouncerFailedCheckError("self.catalog_node is None")
        if self.manifest_obj is None:
            raise DbtBouncerFailedCheckError("self.manifest_obj is None")
        if self.is_catalog_node_a_model(self.catalog_node, self.models):
            model = next(
                m for m in self.models if m.unique_id == self.catalog_node.unique_id
            )

            if self.manifest_obj.manifest.metadata.adapter_type in ["snowflake"]:
                self.case_sensitive = False

            model_columns = model.columns or {}
            if self.case_sensitive:
                undocumented_columns = [
                    v.name
                    for _, v in self.catalog_node.columns.items()
                    if v.name not in model_columns
                ]
            else:
                undocumented_columns = [
                    v.name
                    for _, v in self.catalog_node.columns.items()
                    if v.name.lower() not in [c.lower() for c in model_columns]
                ]

            if undocumented_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(self.catalog_node.unique_id).split('.')[-1]}` has columns that are not included in the models properties file: {undocumented_columns}"
                )


class CheckColumnsAreDocumentedInPublicModels(BaseCheck):
    """Columns should have a populated description in public models.

    Receives:
        catalog_node (CatalogNodes): The CatalogNodes object to check.
        min_description_length (int | None): Minimum length required for the description to be considered populated.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_columns_are_documented_in_public_models
        ```

    """

    catalog_node: "CatalogNodes | None" = Field(default=None)
    min_description_length: int | None = Field(default=None)
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_columns_are_documented_in_public_models"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If columns are undocumented in public model.

        """
        if self.catalog_node is None:
            raise DbtBouncerFailedCheckError("self.catalog_node is None")
        if self.is_catalog_node_a_model(self.catalog_node, self.models):
            model = next(
                m for m in self.models if m.unique_id == self.catalog_node.unique_id
            )
            non_complying_columns = []
            for _, v in self.catalog_node.columns.items():
                if model.access and model.access.value == "public":
                    model_columns = model.columns or {}
                    column_config = model_columns.get(v.name)
                    if column_config is None or not self._is_description_populated(
                        column_config.description or "", self.min_description_length
                    ):
                        non_complying_columns.append(v.name)

            if non_complying_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(self.catalog_node.unique_id).split('.')[-1]}` is a public model but has columns that don't have a populated description: {non_complying_columns}"
                )
