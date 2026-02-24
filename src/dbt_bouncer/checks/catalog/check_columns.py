import re
from typing import TYPE_CHECKING, Literal

from pydantic import ConfigDict, Field

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

if TYPE_CHECKING:
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
        DbtBouncerTestBase,
    )
from pydantic import PrivateAttr, model_validator

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks._mixins import CatalogNodeMixin, ManifestMixin
from dbt_bouncer.utils import compile_pattern


class CheckColumnDescriptionPopulated(CatalogNodeMixin, ManifestMixin, BaseCheck):
    """Columns must have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

    Receives:
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

    min_description_length: int | None = Field(default=None)
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_column_description_populated"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If description is not populated.

        """
        catalog_node = self._require_catalog_node()
        manifest_obj = self._require_manifest()
        if self.is_catalog_node_a_model(catalog_node, self.models):
            model = next(
                m for m in self.models if m.unique_id == catalog_node.unique_id
            )
            non_complying_columns = []
            for _, v in catalog_node.columns.items():
                # Snowflake saves column descriptions in the 'comment' field in catalog.json
                if manifest_obj.manifest.metadata.adapter_type in ["snowflake"]:
                    description = getattr(v, "comment", "") or ""
                else:
                    columns = model.columns or {}
                    column_from_manifest = columns.get(v.name)
                    description = ""
                    if column_from_manifest:
                        description = column_from_manifest.description or ""

                if not self._is_description_populated(
                    description, self.min_description_length
                ):
                    non_complying_columns.append(v.name)

            if non_complying_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that do not have a populated description: {non_complying_columns}"
                )


class CheckColumnHasSpecifiedTest(CatalogNodeMixin, BaseCheck):
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

    column_name_pattern: str
    name: Literal["check_column_has_specified_test"]
    test_name: str
    tests: list["DbtBouncerTestBase"] = Field(default=[])

    _compiled_column_name_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_column_name_pattern = compile_pattern(
            self.column_name_pattern.strip()
        )

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If column does not have specified test.

        """
        catalog_node = self._require_catalog_node()
        columns_to_check = [
            v.name
            for _, v in catalog_node.columns.items()
            if self._compiled_column_name_pattern.match(str(v.name)) is not None
        ]
        tested_columns = set()
        for t in self.tests:
            test_metadata = getattr(t, "test_metadata", None)
            attached_node = getattr(t, "attached_node", None)
            if (
                test_metadata
                and attached_node
                and getattr(test_metadata, "name", None) == self.test_name
                and attached_node == catalog_node.unique_id
            ):
                tested_columns.add(getattr(t, "column_name", ""))
        non_complying_columns = [c for c in columns_to_check if c not in tested_columns]

        if non_complying_columns:
            raise DbtBouncerFailedCheckError(
                f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that should have a `{self.test_name}` test: {non_complying_columns}"
            )


class CheckColumnNameCompliesToColumnType(CatalogNodeMixin, BaseCheck):
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

    column_name_pattern: str
    name: Literal["check_column_name_complies_to_column_type"]
    type_pattern: str | None = None
    types: list[str] | None = None

    _compiled_column_name_pattern: re.Pattern[str] = PrivateAttr()
    _compiled_type_pattern: re.Pattern[str] | None = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex patterns once at initialisation time."""
        self._compiled_column_name_pattern = compile_pattern(
            self.column_name_pattern.strip()
        )
        if self.type_pattern:
            self._compiled_type_pattern = compile_pattern(self.type_pattern.strip())

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If column name does not comply to column type.

        """
        catalog_node = self._require_catalog_node()
        if self.type_pattern:
            non_complying_columns = [
                v.name
                for _, v in catalog_node.columns.items()
                if self._compiled_type_pattern.match(str(v.type)) is None
                and self._compiled_column_name_pattern.match(str(v.name)) is not None
            ]

            if non_complying_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that don't comply with the specified data type regexp pattern (`{self.column_name_pattern}`): {non_complying_columns}"
                )

        elif self.types:
            non_complying_columns = [
                v.name
                for _, v in catalog_node.columns.items()
                if v.type in self.types
                and self._compiled_column_name_pattern.match(str(v.name)) is None
            ]

            if non_complying_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that don't comply with the specified regexp pattern (`{self.column_name_pattern}`): {non_complying_columns}"
                )

    @model_validator(mode="after")
    def _check_type_pattern_or_types(self) -> "CheckColumnNameCompliesToColumnType":
        if not (self.type_pattern or self.types):
            raise ValueError("Either 'type_pattern' or 'types' must be supplied.")
        if self.type_pattern is not None and self.types is not None:
            raise ValueError("Only one of 'type_pattern' or 'types' can be supplied.")
        return self


class CheckColumnNames(CatalogNodeMixin, BaseCheck):
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

    column_name_pattern: str
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_column_names"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If column name does not match regex.

        """
        catalog_node = self._require_catalog_node()
        if self.is_catalog_node_a_model(catalog_node, self.models):
            non_complying_columns: list[str] = []
            non_complying_columns.extend(
                v.name
                for _, v in catalog_node.columns.items()
                if re.fullmatch(self.column_name_pattern.strip(), str(v.name)) is None
            )

            if non_complying_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns ({non_complying_columns}) that do not match the supplied regex: `{self.column_name_pattern.strip()}`."
                )


class CheckColumnsAreAllDocumented(CatalogNodeMixin, ManifestMixin, BaseCheck):
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
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_columns_are_all_documented"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If columns are undocumented.

        """
        catalog_node = self._require_catalog_node()
        manifest_obj = self._require_manifest()
        if self.is_catalog_node_a_model(catalog_node, self.models):
            model = next(
                m for m in self.models if m.unique_id == catalog_node.unique_id
            )

            if manifest_obj.manifest.metadata.adapter_type in ["snowflake"]:
                self.case_sensitive = False

            model_columns = model.columns or {}
            if self.case_sensitive:
                undocumented_columns = [
                    v.name
                    for _, v in catalog_node.columns.items()
                    if v.name not in model_columns
                ]
            else:
                model_columns_lower = {c.lower() for c in model_columns}
                undocumented_columns = [
                    v.name
                    for _, v in catalog_node.columns.items()
                    if v.name.lower() not in model_columns_lower
                ]

            if undocumented_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that are not included in the models properties file: {undocumented_columns}"
                )


class CheckColumnsAreDocumentedInPublicModels(CatalogNodeMixin, BaseCheck):
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

    min_description_length: int | None = Field(default=None)
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_columns_are_documented_in_public_models"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If columns are undocumented in public model.

        """
        catalog_node = self._require_catalog_node()
        if self.is_catalog_node_a_model(catalog_node, self.models):
            model = next(
                m for m in self.models if m.unique_id == catalog_node.unique_id
            )
            non_complying_columns = []
            for _, v in catalog_node.columns.items():
                if model.access and model.access.value == "public":
                    model_columns = model.columns or {}
                    column_config = model_columns.get(v.name)
                    if column_config is None or not self._is_description_populated(
                        column_config.description or "", self.min_description_length
                    ):
                        non_complying_columns.append(v.name)

            if non_complying_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{str(catalog_node.unique_id).split('.')[-1]}` is a public model but has columns that don't have a populated description: {non_complying_columns}"
                )
