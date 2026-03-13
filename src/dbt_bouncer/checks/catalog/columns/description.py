"""Checks related to column descriptions and documentation coverage."""

from typing import Any

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import is_description_populated


def _is_catalog_node_a_model(catalog_node: Any, models: list[Any]) -> bool:
    """Return True if a catalog node corresponds to a dbt model.

    Returns:
        bool: Whether a catalog node is a model.

    """
    model = next((m for m in models if m.unique_id == catalog_node.unique_id), None)
    return model is not None and model.resource_type == "model"


@check(
    "check_column_description_populated",
    iterate_over="catalog_node",
    params={"min_description_length": (int | None, None)},
)
def check_column_description_populated(
    catalog_node, ctx, *, min_description_length: int | None
):
    """Columns must have a populated description."""
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


@check(
    "check_columns_are_all_documented",
    iterate_over="catalog_node",
    params={"case_sensitive": (bool | None, True)},
)
def check_columns_are_all_documented(catalog_node, ctx, *, case_sensitive: bool | None):
    """All columns in a model should be included in the model's properties file."""
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


@check(
    "check_columns_are_documented_in_public_models",
    iterate_over="catalog_node",
    params={"min_description_length": (int | None, None)},
)
def check_columns_are_documented_in_public_models(
    catalog_node, ctx, *, min_description_length: int | None
):
    """Columns should have a populated description in public models."""
    if _is_catalog_node_a_model(catalog_node, ctx.models):
        model = next(m for m in ctx.models if m.unique_id == catalog_node.unique_id)
        non_complying_columns = []
        for _, v in catalog_node.columns.items():
            if model.access and model.access.value == "public":
                model_columns = model.columns or {}
                column_config = model_columns.get(v.name)
                if column_config is None or not is_description_populated(
                    column_config.description or "",
                    min_description_length or 4,
                ):
                    non_complying_columns.append(v.name)

        if non_complying_columns:
            fail(
                f"`{str(catalog_node.unique_id).split('.')[-1]}` is a public model but has columns that don't have a populated description: {non_complying_columns}"
            )
