"""Checks related to column tests."""

import re

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import compile_pattern


@check
def check_column_has_specified_test(
    catalog_node,
    ctx,
    *,
    column_name_pattern: str,
    test_name: str,
    case_sensitive: bool | None = True,
):
    """Columns that match the specified regexp pattern must have a specified test.

    !!! info "Rationale"

        Naming conventions communicate expectations: a column named `is_active` implies it is boolean and never null; a column ending in `_id` implies it is a valid foreign key. Without enforcement, these implicit contracts go untested, and referential integrity issues or null values can silently corrupt downstream aggregations. This check bridges naming conventions and data quality by automatically requiring specific tests on columns that match a pattern, eliminating the manual overhead of reviewing every column individually.

    Parameters:
        column_name_pattern (str): Regex pattern to match the column name.
        test_name (str): Name of the test to check for.

    Receives:
        case_sensitive (bool | None): Whether the column names are case sensitive or not. Necessary for adapters like `dbt-snowflake` where the column in `catalog.json` is uppercase but the column in `manifest.json` can be lowercase. Defaults to `false` for `dbt-snowflake`, otherwise `true`.
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.
        manifest_obj (ManifestObject): The ManifestObject object parsed from `manifest.json`.
        tests (list[TestNode]): List of TestNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_column_has_specified_test
              column_name_pattern: ^is_.*
              test_name: not_null
        ```

    """
    if ctx.manifest_obj.manifest.metadata.adapter_type in ["snowflake"]:
        case_sensitive = False

    # When matching is case-insensitive the catalog name is upper-cased (e.g.
    # Snowflake) while patterns are conventionally written lowercase, so the regex
    # must ignore case too — otherwise the column silently falls out of scope.
    pattern_flags = 0 if case_sensitive else re.IGNORECASE
    compiled_column_name_pattern = compile_pattern(
        column_name_pattern.strip(), pattern_flags
    )
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
            column_name = getattr(t, "column_name", "")
            # A column-level test exposes the documented (YAML) column name; model-level
            # tests have no `column_name`, so skip them rather than adding an empty entry.
            if column_name:
                tested_columns.add(column_name)

    if case_sensitive:
        non_complying_columns = [c for c in columns_to_check if c not in tested_columns]
    else:
        # On case-folding adapters (e.g. Snowflake) the catalog column is uppercase
        # while the test's column name mirrors the lowercase YAML, so compare lowered.
        tested_columns_lower = {c.lower() for c in tested_columns}
        non_complying_columns = [
            c for c in columns_to_check if c.lower() not in tested_columns_lower
        ]

    if non_complying_columns:
        fail(
            f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that should have a `{test_name}` test: {non_complying_columns}"
        )
