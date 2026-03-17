"""Checks related to model column definitions, types, and constraints."""

import re

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.checks.common import NestedDict
from dbt_bouncer.enums import Materialization
from dbt_bouncer.utils import find_missing_meta_keys, get_clean_model_name


@check
def check_model_columns_have_relationship_tests(
    model,
    ctx,
    *,
    column_name_pattern: str,
    target_column_pattern: str | None = None,
    target_model_pattern: str | None = None,
):
    """Columns matching a regex pattern must have a `relationships` test, optionally validating the target column and model.

    Parameters:
        column_name_pattern (str): Regex pattern to match column names that require a relationships test.
        target_column_pattern (str | None): Regex pattern the target column (`field`) of the relationships test must match. If not provided, any target column is accepted.
        target_model_pattern (str | None): Regex pattern the target model of the relationships test must match. If not provided, any target model is accepted.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_columns_have_relationship_tests
              column_name_pattern: "_fk$"
        ```
        ```yaml
        manifest_checks:
            - name: check_model_columns_have_relationship_tests
              column_name_pattern: "_fk$"
              target_column_pattern: "_pk$"
              target_model_pattern: "^dim_|^fact_"
        ```

    """
    columns = model.columns or {}
    failing_columns: dict[str, str] = {}

    # Find all relationships tests attached to this model
    relationship_tests = []
    for test in ctx.tests:
        test_metadata = getattr(test, "test_metadata", None)
        attached_node = getattr(test, "attached_node", None)
        if (
            test_metadata
            and attached_node == model.unique_id
            and getattr(test_metadata, "name", "") == "relationships"
        ):
            relationship_tests.append(test_metadata)

    for col_name in columns:
        if not re.search(column_name_pattern, col_name):
            continue

        # Find a relationships test for this column
        matching_test = None
        for test_meta in relationship_tests:
            kwargs = getattr(test_meta, "kwargs", {}) or {}
            if isinstance(kwargs, dict):
                test_col = kwargs.get("column_name", "")
            else:
                test_col = getattr(kwargs, "column_name", "")
            if test_col == col_name:
                matching_test = test_meta
                break

        if matching_test is None:
            failing_columns[col_name] = "no relationships test found"
            continue

        kwargs = getattr(matching_test, "kwargs", {}) or {}
        if isinstance(kwargs, dict):
            target_field = kwargs.get("field", "")
            target_to = kwargs.get("to", "")
        else:
            target_field = getattr(kwargs, "field", "")
            target_to = getattr(kwargs, "to", "")

        if target_column_pattern and not re.search(target_column_pattern, target_field):
            failing_columns[col_name] = (
                f'target column "{target_field}" does not match pattern "{target_column_pattern}"'
            )
            continue

        if target_model_pattern:
            # Extract model name from ref('model_name') or source('source', 'table')
            ref_match = re.search(r"ref\(['\"](\w+)['\"]\)", target_to)
            target_model_name = ref_match.group(1) if ref_match else target_to
            if not re.search(target_model_pattern, target_model_name):
                failing_columns[col_name] = (
                    f'target model "{target_model_name}" does not match pattern "{target_model_pattern}"'
                )

    if failing_columns:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has columns missing required `relationships` tests: {failing_columns}"
        )


@check
def check_model_columns_have_meta_keys(model, *, keys: NestedDict):
    """Columns defined for models must have the specified keys in the `meta` config.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_columns_have_meta_keys
              keys:
                - owner
                - pii
        ```

    """
    columns = model.columns or {}
    failing_columns: dict[str, list[str]] = {}
    for col_name, col in columns.items():
        missing_keys = find_missing_meta_keys(
            meta_config=col.meta or {}, required_keys=keys.model_dump()
        )
        if missing_keys:
            failing_columns[col_name] = [k.replace(">>", "") for k in missing_keys]
    if failing_columns:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has columns missing required `meta` keys: {failing_columns}"
        )


@check
def check_model_columns_have_types(model):
    """Columns defined for models must have a `data_type` declared.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_columns_have_types
              include: ^models/marts
        ```

    """
    columns = model.columns or {}
    untyped_columns = [
        col_name for col_name, col in columns.items() if not col.data_type
    ]
    if untyped_columns:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has columns without a declared `data_type`: {untyped_columns}"
        )


@check
def check_model_has_constraints(model, *, required_constraint_types: list[str]):
    """Table and incremental models must have the specified constraint types defined.

    Parameters:
        required_constraint_types (list[Literal["check", "custom", "foreign_key", "not_null", "primary_key", "unique"]]): List of constraint types that must be present on the model.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_constraints
              required_constraint_types:
                - primary_key
              include: ^models/marts
        ```

    """
    materialization = (
        model.config.materialized
        if model.config and hasattr(model.config, "materialized")
        else None
    )
    if materialization not in (Materialization.TABLE, Materialization.INCREMENTAL):
        return
    constraints = model.constraints or []
    actual_types: set[str] = set()
    for c in constraints:
        c_type = getattr(c, "type")  # noqa: B009 - avoids ty shadowing of builtin `type`
        actual_types.add(c_type.value if hasattr(c_type, "value") else str(c_type))
    missing_types = sorted(set(required_constraint_types) - actual_types)
    if missing_types:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is missing required constraint types: {missing_types}"
        )
