"""Checks related to column descriptions and documentation coverage."""

from typing import TYPE_CHECKING, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
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
    )


def _is_catalog_node_a_model(
    catalog_node: "CatalogNodes", models: list["DbtBouncerModelBase"]
) -> bool:
    """Return True if a catalog node corresponds to a dbt model.

    Args:
        catalog_node (CatalogNodes): The CatalogNodes object to check.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Returns:
        bool: Whether a catalog node is a model.

    """
    model = next((m for m in models if m.unique_id == catalog_node.unique_id), None)
    return model is not None and model.resource_type == "model"


class CheckColumnDescriptionPopulated(BaseCheck):
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

    catalog_node: "CatalogNodes | None" = Field(default=None)
    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
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
        if _is_catalog_node_a_model(catalog_node, self.models):
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
        catalog_node = self._require_catalog_node()
        manifest_obj = self._require_manifest()
        if _is_catalog_node_a_model(catalog_node, self.models):
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
        catalog_node = self._require_catalog_node()
        if _is_catalog_node_a_model(catalog_node, self.models):
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
