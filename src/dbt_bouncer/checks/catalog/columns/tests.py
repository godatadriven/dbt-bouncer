"""Checks related to column tests."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern


@check
def check_column_has_specified_test(
    catalog_node, ctx, *, column_name_pattern: str, test_name: str
):
    """Columns that match the specified regexp pattern must have a specified test."""
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
