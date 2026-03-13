"""Checks related to column naming conventions."""

import re
from typing import Any

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern


def _is_catalog_node_a_model(catalog_node: Any, models: list[Any]) -> bool:
    """Return True if a catalog node corresponds to a dbt model.

    Returns:
        bool: Whether a catalog node is a model.

    """
    model = next((m for m in models if m.unique_id == catalog_node.unique_id), None)
    return model is not None and model.resource_type == "model"


@check(
    "check_column_name_complies_to_column_type",
    iterate_over="catalog_node",
    params={
        "column_name_pattern": str,
        "type_pattern": (str | None, None),
        "types": (list[str] | None, None),
    },
)
def check_column_name_complies_to_column_type(
    catalog_node,
    ctx,
    *,
    column_name_pattern: str,
    type_pattern: str | None,
    types: list[str] | None,
):
    """Columns with the specified regexp naming pattern must have compliant data types.

    Raises:
        ValueError: If neither or both of type_pattern/types are supplied.

    """
    if not (type_pattern or types):
        msg = "Either 'type_pattern' or 'types' must be supplied."
        raise ValueError(msg)
    if type_pattern is not None and types is not None:
        msg = "Only one of 'type_pattern' or 'types' can be supplied."
        raise ValueError(msg)

    compiled_column_name_pattern = compile_pattern(column_name_pattern.strip())

    if type_pattern:
        compiled_type_pattern = compile_pattern(type_pattern.strip())
        non_complying_columns = [
            v.name
            for _, v in catalog_node.columns.items()
            if compiled_type_pattern.match(str(v.type)) is None
            and compiled_column_name_pattern.match(str(v.name)) is not None
        ]

        if non_complying_columns:
            fail(
                f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that don't comply with the specified data type regexp pattern (`{column_name_pattern}`): {non_complying_columns}"
            )

    elif types:
        non_complying_columns = [
            v.name
            for _, v in catalog_node.columns.items()
            if v.type in types
            and compiled_column_name_pattern.match(str(v.name)) is None
        ]

        if non_complying_columns:
            fail(
                f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that don't comply with the specified regexp pattern (`{column_name_pattern}`): {non_complying_columns}"
            )


@check(
    "check_column_names",
    iterate_over="catalog_node",
    params={"column_name_pattern": str},
)
def check_column_names(catalog_node, ctx, *, column_name_pattern: str):
    """Columns must have a name that matches the supplied regex."""
    if _is_catalog_node_a_model(catalog_node, ctx.models):
        non_complying_columns: list[str] = []
        non_complying_columns.extend(
            v.name
            for _, v in catalog_node.columns.items()
            if re.fullmatch(column_name_pattern.strip(), str(v.name)) is None
        )

        if non_complying_columns:
            fail(
                f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns ({non_complying_columns}) that do not match the supplied regex: `{column_name_pattern.strip()}`."
            )
