"""Checks related to column tests."""

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import compile_pattern


@check
def check_column_has_specified_test(
    catalog_node, ctx, *, column_name_pattern: str, test_name: str
):
    """Columns that match the specified regexp pattern must have a specified test.

    !!! info "Rationale"

        Naming conventions communicate expectations: a column named `is_active` implies it is boolean and never null; a column ending in `_id` implies it is a valid foreign key. Without enforcement, these implicit contracts go untested, and referential integrity issues or null values can silently corrupt downstream aggregations. This check bridges naming conventions and data quality by automatically requiring specific tests on columns that match a pattern, eliminating the manual overhead of reviewing every column individually.

    Parameters:
        column_name_pattern (str): Regex pattern to match the column name.
        test_name (str): Name of the test to check for.

    Receives:
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.
        tests (list[TestNode]): List of TestNode objects parsed from `manifest.json`.

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
    compiled_column_name_pattern = compile_pattern(column_name_pattern.strip())
    columns_to_check = [
        v.name
        for _, v in catalog_node.columns.items()
        if compiled_column_name_pattern.match(str(v.name)) is not None
    ]
    tested_columns = set()
    for t in ctx.tests:
        test_metadata = getattr(t, "test_metadata", None)
        attached_node = getattr(t, "attached_node", None)
        if (
            test_metadata
            and attached_node
            and getattr(test_metadata, "name", None) == test_name
            and attached_node == catalog_node.unique_id
        ):
            tested_columns.add(getattr(t, "column_name", ""))
    non_complying_columns = [c for c in columns_to_check if c not in tested_columns]

    if non_complying_columns:
        fail(
            f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that should have a `{test_name}` test: {non_complying_columns}"
        )
