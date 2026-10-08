from dbt_bouncer.artifact_parsers.info_schema import InfoSchema
from dbt_bouncer.testing import check_fails, check_passes

MODEL = "model.package_name.model_1"
SOURCE = "source.package_name.source_1.table_1"
SOURCE_COLUMNS = {"id": {"name": "id"}, "legacy_flag": {"name": "legacy_flag"}}


def _edge(parent_column, child=MODEL, evolution="copy"):
    return {
        "parent_node_unique_id": SOURCE,
        "parent_column_name": parent_column,
        "child_node_unique_id": child,
        "child_column_name": parent_column,
        "evolution": evolution,
    }


def _run(check_fn, edges, models=None, **kwargs):
    check_fn(
        "check_source_columns_are_used",
        source={"unique_id": SOURCE, "columns": SOURCE_COLUMNS},
        ctx_info_schema=InfoSchema.from_rows(column_lineage=edges),
        ctx_models=(
            models
            if models is not None
            else [{"unique_id": MODEL, "depends_on": {"nodes": [SOURCE]}}]
        ),
        **kwargs,
    )


def test_all_columns_used_passes():
    _run(check_passes, [_edge("id"), _edge("legacy_flag", evolution="scan")])


def test_unused_column_fails():
    _run(check_fails, [_edge("ID")], match="`legacy_flag`")


def test_child_without_lineage_is_not_checked():
    """A child model that static analysis skipped could use any column."""
    other = "model.package_name.model_2"
    _run(
        check_passes,
        [_edge("id")],
        models=[
            {"unique_id": MODEL, "depends_on": {"nodes": [SOURCE]}},
            {"unique_id": other, "depends_on": {"nodes": [SOURCE]}},
        ],
    )


def test_source_without_children_passes():
    _run(check_passes, [], models=[])


def test_ephemeral_child_without_lineage_is_ignored():
    """Dbt records an ephemeral model's lineage on its consumers."""
    ephemeral = "model.package_name.model_2"
    _run(
        check_fails,
        [_edge("id")],
        models=[
            {"unique_id": MODEL, "depends_on": {"nodes": [SOURCE]}},
            {
                "unique_id": ephemeral,
                "config": {"materialized": "ephemeral"},
                "depends_on": {"nodes": [SOURCE]},
            },
        ],
        match="`legacy_flag`",
    )
