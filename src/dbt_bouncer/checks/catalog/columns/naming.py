"""Checks related to column naming conventions."""

import re
from typing import Any

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import compile_pattern


def _is_catalog_node_a_model(catalog_node: Any, models: list[Any]) -> bool:
    """Return True if a catalog node corresponds to a dbt model.

    Returns:
        bool: Whether a catalog node is a model.

    """
    model = next((m for m in models if m.unique_id == catalog_node.unique_id), None)
    return model is not None and model.resource_type == "model"


@check
def check_column_name_complies_to_column_type(
    catalog_node,
    *,
    column_name_pattern: str,
    type_pattern: str | None = None,
    types: list[str] | None = None,
):
    """Columns with the specified regexp naming pattern must have data types that comply to the specified regexp pattern or list of data types.

    !!! info "Rationale"

        Naming conventions that encode data types (e.g. `is_` prefix for booleans, `_date` suffix for dates, `_id` suffix for integers) are a common and effective way to make schemas self-describing. Without enforcement, these conventions drift over time: a column named `is_active` might be stored as an integer in one model and a boolean in another, causing silent cast errors downstream. This check ties naming patterns to data types, catching mismatches at CI time rather than in production queries.

    Note: One of `type_pattern` or `types` must be specified.

    Raises:
        ValueError: If neither or both of type_pattern/types are supplied.

    Parameters:
        column_name_pattern (str): Regex pattern to match the model name.
        type_pattern (str | None): Regex pattern to match the data types.
        types (list[str] | None): List of data types to check.

    Receives:
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.

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


@check
def check_column_names(catalog_node, ctx, *, column_name_pattern: str):
    """Columns must have a name that matches the supplied regex.

    !!! info "Rationale"

        Consistent column naming is the foundation of a readable and maintainable dbt project. Inconsistent casing, abbreviations, or special characters make SQL harder to write, cause join errors, and confuse data consumers who query the warehouse directly. A single enforced naming pattern (e.g. `^[a-z_]*$` for snake_case) eliminates an entire class of stylistic bugs and ensures that columns look the same whether viewed in dbt docs, a BI tool, or a raw SQL editor.

    Parameters:
        column_name_pattern (str): Regexp the column name must match.

    Receives:
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.

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
