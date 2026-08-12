"""Checks related to model column definitions, types, and constraints."""

import re
from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.check_framework.exceptions import NestedDict
from dbt_bouncer.enums import Materialization
from dbt_bouncer.utils import (
    compile_pattern,
    find_missing_meta_keys,
    get_clean_model_name,
    is_description_populated,
)


@check(code="MO015")
def check_model_columns_have_relationship_tests(
    model,
    ctx,
    *,
    column_name_pattern: str,
    target_column_pattern: str | None = None,
    target_model_pattern: str | None = None,
):
    """Columns matching a regex pattern must have a `relationships` test, optionally validating the target column and model.

    !!! info "Rationale"

        Foreign-key columns that are never validated with a `relationships` test can silently contain orphaned IDs, leading to incorrect join results and data quality issues that are hard to trace. This check ensures that columns following a naming convention (e.g. `_fk`) are always backed by a referential integrity test.

    Parameters:
        column_name_pattern (str): Regex pattern to match column names that require a relationships test.
        target_column_pattern (str | None): Regex pattern the target column (`field`) of the relationships test must match. If not provided, any target column is accepted.
        target_model_pattern (str | None): Regex pattern the target model of the relationships test must match. If not provided, any target model is accepted.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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
    for test in ctx.tests_by_attached_node.get(model.unique_id, []):
        test_metadata = getattr(test, "test_metadata", None)
        if test_metadata and getattr(test_metadata, "name", "") == "relationships":
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


@check(code="MO014")
def check_model_columns_have_meta_keys(model, *, keys: NestedDict):
    """Columns defined for models must have the specified keys in the `meta` config.

    !!! info "Rationale"

        Column-level metadata such as `owner` or `pii` flags is essential for data governance, access control, and cataloguing. Without enforcement, metadata is applied inconsistently, making it difficult to identify sensitive columns or assign accountability across a large project.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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


@check(code="MO016")
def check_model_columns_have_types(model):
    """Columns defined for models must have a `data_type` declared.

    !!! info "Rationale"

        Declaring column data types is a prerequisite for enforced dbt contracts and enables downstream consumers to understand the expected format of each field without querying the warehouse. It also prevents type-mismatch errors in tools that consume the schema at build time.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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


@check(code="MO017")
def check_model_has_constraints(model, *, required_constraint_types: list[str]):
    """Table and incremental models must have the specified constraint types defined.

    !!! info "Rationale"

        Database constraints such as `primary_key` and `not_null` enforce data integrity at the warehouse level, providing a safety net that goes beyond dbt tests. Requiring them on materialised models ensures that quality guarantees survive even when dbt tests are skipped or not run on every refresh.

    Parameters:
        required_constraint_types (list[Literal["check", "custom", "foreign_key", "not_null", "primary_key", "unique"]]): List of constraint types that must be present on the model.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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
        c_type = getattr(c, "type")  # ruff: ignore[get-attr-with-constant] - avoids ty shadowing of builtin `type`
        actual_types.add(c_type.value if hasattr(c_type, "value") else str(c_type))
    missing_types = sorted(set(required_constraint_types) - actual_types)
    if missing_types:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is missing required constraint types: {missing_types}"
        )


@check(code="MO018")
def check_model_single_primary_key(model):
    """Models must have at most one column-level primary key constraint.

    !!! info "Rationale"

        Declaring more than one column-level primary-key constraint in a dbt model is almost always a modelling mistake — it implies multiple independent identity columns, which is semantically ambiguous and can confuse downstream consumers. This check flags models with two or more column-level `primary_key` constraints so the author can consolidate them into a single-column PK or a composite constraint at model level.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_single_primary_key
        ```

    """
    columns = model.columns or {}
    pk_columns: list[str] = []
    for col_name, col in columns.items():
        constraints = getattr(col, "constraints", None) or []
        for c in constraints:
            c_type = getattr(c, "type", None)
            type_str = c_type.value if hasattr(c_type, "value") else str(c_type or "")
            if type_str == "primary_key":
                pk_columns.append(col_name)
                break
    if len(pk_columns) > 1:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has more than one column-level primary key constraint: {pk_columns}"
        )


@check(code="MO053")
def check_model_column_has_specified_test(
    model,
    ctx,
    *,
    column_name_pattern: str,
    test_name: str,
):
    """Columns declared in a model's properties file that match the specified regexp pattern must have a specified test.

    !!! info "Rationale"

        Naming conventions communicate expectations: a column named `is_active` implies it is boolean and never null; a column ending in `_id` implies it is a valid foreign key. Without enforcement, these implicit contracts go untested, and referential integrity issues or null values can silently corrupt downstream aggregations. This check bridges naming conventions and data quality by automatically requiring specific tests on columns that match a pattern, eliminating the manual overhead of reviewing every column individually.

    This is the manifest-only analogue of `check_column_has_specified_test`, which requires `catalog.json`. It only evaluates columns declared in `model.columns`, i.e. columns present in the model's properties file; columns that exist in the warehouse but are not documented are invisible to this check. Unlike the catalog check it has no `case_sensitive` parameter: both the column names and the tests are read from `manifest.json`, so no cross-artifact casing mismatch is possible.

    Parameters:
        column_name_pattern (str): Regex pattern to match the column name.
        test_name (str): Name of the test to check for.

    Receives:
        model (ModelNode): The ModelNode object to check.
        tests (list[TestNode]): List of TestNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_column_has_specified_test
              column_name_pattern: ^is_.*
              test_name: not_null
        ```

    """
    compiled_column_name_pattern = compile_pattern(column_name_pattern.strip())
    columns = model.columns or {}
    columns_to_check = [
        col_name
        for col_name in columns
        if compiled_column_name_pattern.match(str(col_name)) is not None
    ]

    tested_columns = set()
    for t in ctx.tests_by_attached_node.get(model.unique_id, []):
        test_metadata = getattr(t, "test_metadata", None)
        if test_metadata and getattr(test_metadata, "name", None) == test_name:
            column_name = getattr(t, "column_name", "")
            # A column-level test exposes the documented (YAML) column name; model-level
            # tests have no `column_name`, so skip them rather than adding an empty entry.
            if column_name:
                tested_columns.add(column_name)

    non_complying_columns = [c for c in columns_to_check if c not in tested_columns]
    if non_complying_columns:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has columns that should have a `{test_name}` test: {non_complying_columns}"
        )


@check(code="MO054")
def check_model_column_description_populated(
    model,
    *,
    min_description_length: Annotated[int, Field(gt=0)] | None = None,
):
    """Columns declared in a model's properties file must have a populated description.

    !!! info "Rationale"

        Column-level documentation is where data consumers spend most of their time: understanding what `is_active` means, whether `amount` is in cents or pounds, or which ID to join on. Without column descriptions, analysts guess, make mistakes, and create conflicting metrics. This check ensures every column is explained, which is especially valuable for data catalogues and BI tool integrations that surface these descriptions automatically.

    This is the manifest-only analogue of `check_column_description_populated`, which requires `catalog.json`. It only evaluates columns declared in `model.columns`, i.e. columns present in the model's properties file; use the catalog check `check_columns_are_all_documented` to detect columns that exist in the warehouse but are undocumented.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_column_description_populated
              include: ^models/marts
        ```
        ```yaml
        manifest_checks:
            - name: check_model_column_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """
    columns = model.columns or {}
    non_complying_columns = [
        col_name
        for col_name, col in columns.items()
        if not is_description_populated(
            col.description or "", min_description_length or 4
        )
    ]

    if non_complying_columns:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has columns that do not have a populated description: {non_complying_columns}"
        )


@check(code="MO055")
def check_model_column_name_complies_to_column_type(
    model,
    *,
    column_name_pattern: str,
    type_pattern: str | None = None,
    types: list[str] | None = None,
):
    """Columns with the specified regexp naming pattern must have declared data types that comply to the specified regexp pattern or list of data types.

    !!! info "Rationale"

        Naming conventions that encode data types (e.g. `is_` prefix for booleans, `_date` suffix for dates, `_id` suffix for integers) are a common and effective way to make schemas self-describing. Without enforcement, these conventions drift over time: a column named `is_active` might be stored as an integer in one model and a boolean in another, causing silent cast errors downstream. This check ties naming patterns to data types, catching mismatches at CI time rather than in production queries.

    This is the manifest-only analogue of `check_column_name_complies_to_column_type`, which requires `catalog.json`. It only evaluates columns declared in `model.columns` and compares against the `data_type` declared in the properties file, not the type physically introspected from the warehouse. Columns with no declared `data_type` are skipped rather than failed — use `check_model_columns_have_types` to enforce that a type is declared.

    Note: One of `type_pattern` or `types` must be specified.

    Parameters:
        column_name_pattern (str): Regex pattern to match the column name.
        type_pattern (str | None): Regex pattern to match the data types.
        types (list[str] | None): List of data types to check.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Raises:
        ValueError: If neither or both of type_pattern/types are supplied.

    Example(s):
        ```yaml
        manifest_checks:
            # Columns whose names end with "_date" must be of type DATE.
            - name: check_model_column_name_complies_to_column_type
              column_name_pattern: .*_date$
              types:
                - DATE
        ```
        ```yaml
        manifest_checks:
            # Snake-case columns must not be a STRUCT type.
            - name: check_model_column_name_complies_to_column_type
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
    columns = model.columns or {}
    # Columns without a declared `data_type` are skipped: compliance cannot be
    # determined for an undeclared type (see `check_model_columns_have_types`).
    typed_columns = [
        (col_name, col.data_type) for col_name, col in columns.items() if col.data_type
    ]

    if type_pattern:
        compiled_type_pattern = compile_pattern(type_pattern.strip())
        non_complying_columns = [
            col_name
            for col_name, data_type in typed_columns
            if compiled_type_pattern.match(str(data_type)) is None
            and compiled_column_name_pattern.match(str(col_name)) is not None
        ]

        if non_complying_columns:
            fail(
                f"`{get_clean_model_name(model.unique_id)}` has columns matching `{column_name_pattern}` whose declared data type does not match `{type_pattern}`: {non_complying_columns}"
            )

    elif types:
        non_complying_columns = [
            col_name
            for col_name, data_type in typed_columns
            if data_type not in types
            and compiled_column_name_pattern.match(str(col_name)) is not None
        ]

        if non_complying_columns:
            fail(
                f"`{get_clean_model_name(model.unique_id)}` has columns matching `{column_name_pattern}` whose declared data type is not in {types}: {non_complying_columns}"
            )


@check(code="MO056")
def check_model_column_type_complies_to_column_name(
    model,
    *,
    column_name_pattern: str,
    type_pattern: str | None = None,
    types: list[str] | None = None,
):
    """Columns with the specified declared data type must have names that comply to the specified regexp pattern.

    !!! info "Rationale"

        This is the reverse of `check_model_column_name_complies_to_column_type`. While that check ensures columns with a given naming pattern have the correct data type, this check ensures columns with a given data type follow the correct naming convention. For example, you may want all `BOOLEAN` columns to start with `is_` or `has_`, or all `DATE` columns to end with `_date`. Enforcing this direction catches columns that have the right type but the wrong name — a gap the other check cannot cover.

    This is the manifest-only analogue of `check_column_type_complies_to_column_name`, which requires `catalog.json`. It only evaluates columns declared in `model.columns` and compares against the `data_type` declared in the properties file, not the type physically introspected from the warehouse. Columns with no declared `data_type` are skipped rather than failed — use `check_model_columns_have_types` to enforce that a type is declared.

    Note: One of `type_pattern` or `types` must be specified.

    Parameters:
        column_name_pattern (str): Regex pattern that column names must match.
        type_pattern (str | None): Regex pattern to match the data types.
        types (list[str] | None): List of data types to check.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Raises:
        ValueError: If neither or both of type_pattern/types are supplied.

    Example(s):
        ```yaml
        manifest_checks:
            # BOOLEAN columns must start with "is_" or "has_"
            - name: check_model_column_type_complies_to_column_name
              column_name_pattern: ^(is|has)_.*
              types:
                - BOOLEAN
        ```
        ```yaml
        manifest_checks:
            # Integer-like columns must end with "_id" or "_count"
            - name: check_model_column_type_complies_to_column_name
              column_name_pattern: .*((_id)|(_count))$
              types:
                - BIGINT
                - INTEGER
        ```

    """
    if not (type_pattern or types):
        msg = "Either 'type_pattern' or 'types' must be supplied."
        raise ValueError(msg)
    if type_pattern is not None and types is not None:
        msg = "Only one of 'type_pattern' or 'types' can be supplied."
        raise ValueError(msg)

    compiled_column_name_pattern = compile_pattern(column_name_pattern.strip())
    columns = model.columns or {}
    # Columns without a declared `data_type` are skipped: compliance cannot be
    # determined for an undeclared type (see `check_model_columns_have_types`).
    typed_columns = [
        (col_name, col.data_type) for col_name, col in columns.items() if col.data_type
    ]

    if type_pattern:
        compiled_type_pattern = compile_pattern(type_pattern.strip())
        non_complying_columns = [
            col_name
            for col_name, data_type in typed_columns
            if compiled_type_pattern.match(str(data_type)) is not None
            and compiled_column_name_pattern.match(str(col_name)) is None
        ]

        if non_complying_columns:
            fail(
                f"`{get_clean_model_name(model.unique_id)}` has columns with declared types matching `{type_pattern}` that don't comply with the specified naming pattern (`{column_name_pattern}`): {non_complying_columns}"
            )

    elif types:
        non_complying_columns = [
            col_name
            for col_name, data_type in typed_columns
            if data_type in types
            and compiled_column_name_pattern.match(str(col_name)) is None
        ]

        if non_complying_columns:
            fail(
                f"`{get_clean_model_name(model.unique_id)}` has columns with declared types in {types} that don't comply with the specified naming pattern (`{column_name_pattern}`): {non_complying_columns}"
            )


@check(code="MO057")
def check_model_column_names(model, *, column_name_pattern: str):
    """Columns declared in a model's properties file must have a name that matches the supplied regex.

    !!! info "Rationale"

        Consistent column naming is the foundation of a readable and maintainable dbt project. Inconsistent casing, abbreviations, or special characters make SQL harder to write, cause join errors, and confuse data consumers who query the warehouse directly. A single enforced naming pattern (e.g. `^[a-z_]*$` for snake_case) eliminates an entire class of stylistic bugs and ensures that columns look the same whether viewed in dbt docs, a BI tool, or a raw SQL editor.

    This is the manifest-only analogue of `check_column_names`, which requires `catalog.json`. It only evaluates columns declared in `model.columns`, i.e. columns present in the model's properties file; columns that exist in the warehouse but are undocumented are invisible to this check.

    Parameters:
        column_name_pattern (str): Regexp the column name must match.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_column_names
              column_name_pattern: [a-z_] # Lowercase only, underscores allowed
        ```

    """
    columns = model.columns or {}
    non_complying_columns = [
        col_name
        for col_name in columns
        if re.fullmatch(column_name_pattern.strip(), str(col_name)) is None
    ]

    if non_complying_columns:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has columns ({non_complying_columns}) that do not match the supplied regex: `{column_name_pattern.strip()}`."
        )
