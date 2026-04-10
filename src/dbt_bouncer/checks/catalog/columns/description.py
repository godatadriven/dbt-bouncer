"""Checks related to column descriptions and documentation coverage."""

from typing import Annotated, Any

from pydantic import Field

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import is_description_populated


def _is_catalog_node_a_model(catalog_node: Any, models: list[Any]) -> bool:
    """Return True if a catalog node corresponds to a dbt model.

    Returns:
        bool: Whether a catalog node is a model.

    """
    model = next((m for m in models if m.unique_id == catalog_node.unique_id), None)
    return model is not None and model.resource_type == "model"


@check
def check_column_description_populated(
    catalog_node,
    ctx,
    *,
    min_description_length: Annotated[int, Field(gt=0)] | None = None,
):
    """Columns must have a populated description.

    !!! info "Rationale"

        Column-level documentation is where data consumers spend most of their time: understanding what `is_active` means, whether `amount` is in cents or pounds, or which ID to join on. Without column descriptions, analysts guess, make mistakes, and create conflicting metrics. This check ensures every column is explained, which is especially valuable for data catalogues and BI tool integrations that surface these descriptions automatically.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

    Receives:
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.
        manifest_obj (ManifestObject): The ManifestObject object parsed from `manifest.json`.
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_column_description_populated
              include: ^models/marts
        ```
        ```yaml
        catalog_checks:
            - name: check_column_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """
    if _is_catalog_node_a_model(catalog_node, ctx.models):
        model = next(m for m in ctx.models if m.unique_id == catalog_node.unique_id)
        non_complying_columns = []
        for _, v in catalog_node.columns.items():
            # Snowflake saves column descriptions in the 'comment' field in catalog.json
            if ctx.manifest_obj.manifest.metadata.adapter_type in ["snowflake"]:
                description = getattr(v, "comment", "") or ""
            else:
                columns = model.columns or {}
                column_from_manifest = columns.get(v.name)
                description = ""
                if column_from_manifest:
                    description = column_from_manifest.description or ""

            if not is_description_populated(description, min_description_length or 4):
                non_complying_columns.append(v.name)

        if non_complying_columns:
            fail(
                f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that do not have a populated description: {non_complying_columns}"
            )


@check
def check_columns_are_all_documented(
    catalog_node, ctx, *, case_sensitive: bool | None = True
):
    """All columns in a model should be included in the model's properties file, i.e. `.yml` file.

    !!! info "Rationale"

        When a column exists in the database but is missing from the model's properties file, it cannot receive a description, a data test, or a constraint. These invisible columns accumulate over time as schemas evolve, creating gaps in data quality coverage and making it harder for consumers to discover what data is available. This check ensures the properties file stays in sync with the actual schema, giving teams a complete and testable view of every model.

    Receives:
        case_sensitive (bool | None): Whether the column names are case sensitive or not. Necessary for adapters like `dbt-snowflake` where the column in `catalog.json` is uppercase but the column in `manifest.json` can be lowercase. Defaults to `false` for `dbt-snowflake`, otherwise `true`.
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.
        manifest_obj (ManifestObject): The ManifestObject object parsed from `manifest.json`.
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

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
    if _is_catalog_node_a_model(catalog_node, ctx.models):
        model = next(m for m in ctx.models if m.unique_id == catalog_node.unique_id)

        if ctx.manifest_obj.manifest.metadata.adapter_type in ["snowflake"]:
            case_sensitive = False

        model_columns = model.columns or {}
        if case_sensitive:
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
            fail(
                f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that are not included in the models properties file: {undocumented_columns}"
            )


@check
def check_columns_are_documented_in_public_models(
    catalog_node,
    ctx,
    *,
    min_description_length: Annotated[int, Field(gt=0)] | None = None,
):
    """Columns should have a populated description in public models.

    !!! info "Rationale"

        Public models form the stable, documented API of a dbt project — they are the models that external consumers, BI tools, and other projects are expected to query directly. Undocumented columns in a public model undermine this contract: consumers cannot tell what a field means, which ID to join on, or whether a flag is nullable. Requiring column descriptions on public models ensures the published interface is self-explanatory and trustworthy, which is especially important as teams scale and data catalogues surface model metadata automatically.

    Receives:
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.
        min_description_length (int | None): Minimum length required for the description to be considered populated.
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

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
    if _is_catalog_node_a_model(catalog_node, ctx.models):
        model = next(m for m in ctx.models if m.unique_id == catalog_node.unique_id)
        non_complying_columns = []
        for _, v in catalog_node.columns.items():
            if model.access and model.access.value == "public":
                model_columns = model.columns or {}
                column_config = model_columns.get(v.name)
                if column_config is None or not is_description_populated(
                    column_config.description or "", min_description_length or 4
                ):
                    non_complying_columns.append(v.name)

        if non_complying_columns:
            fail(
                f"`{str(catalog_node.unique_id).split('.')[-1]}` is a public model but has columns that don't have a populated description: {non_complying_columns}"
            )
