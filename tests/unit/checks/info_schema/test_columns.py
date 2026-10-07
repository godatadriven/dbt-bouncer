import pytest

from dbt_bouncer.artifact_parsers.info_schema import InfoSchema
from dbt_bouncer.testing import check_fails, check_passes

MODEL = "model.package_name.model_1"
PARENT = "model.package_name.parent"
SOURCE = "source.package_name.source_1.table_1"


def _edge(parent, parent_column, child, child_column, evolution="copy"):
    return {
        "parent_node_unique_id": parent,
        "parent_column_name": parent_column,
        "child_node_unique_id": child,
        "child_column_name": child_column,
        "evolution": evolution,
    }


def _column(node, name, **fields):
    return {"node_unique_id": node, "column_name": name, **fields}


def _manifest_node(unique_id, columns):
    """Build a manifest node whose columns carry the given `meta` dicts.

    Returns:
        dict: The manifest node.

    """
    return {
        "unique_id": unique_id,
        "columns": {
            name: {"name": name, "meta": meta} for name, meta in columns.items()
        },
    }


class TestCheckModelColumnDescriptionsPropagated:
    @pytest.mark.parametrize(
        ("child_description", "parent_description", "evolution", "check_fn"),
        [
            pytest.param(
                "Order id.", "Order id.", "copy", check_passes, id="both_documented"
            ),
            pytest.param(None, None, "copy", check_passes, id="neither_documented"),
            pytest.param(
                None, "Order id.", "mod", check_passes, id="transformed_column"
            ),
            pytest.param(None, "Order id.", "copy", check_fails, id="description_lost"),
            pytest.param(
                "  ", "Order id.", "copy", check_fails, id="blank_description"
            ),
        ],
    )
    def test_propagation(
        self, child_description, parent_description, evolution, check_fn
    ):
        info_schema = InfoSchema.from_rows(
            column_lineage=[_edge(PARENT, "id", MODEL, "order_id", evolution)],
            node_columns=[
                _column(MODEL, "order_id", description=child_description),
                _column(PARENT, "id", description=parent_description),
            ],
        )
        check_fn(
            "check_model_column_descriptions_propagated",
            model={"unique_id": MODEL},
            ctx_info_schema=info_schema,
        )

    def test_failure_names_column_and_parent(self):
        info_schema = InfoSchema.from_rows(
            column_lineage=[_edge(PARENT, "ID", MODEL, "Order_Id")],
            node_columns=[
                _column(MODEL, "Order_Id"),
                _column(PARENT, "ID", description="Order id."),
            ],
        )
        check_fails(
            "check_model_column_descriptions_propagated",
            match=r"`Order_Id` \(from `model\.package_name\.parent\.ID`\)",
            model={"unique_id": MODEL},
            ctx_info_schema=info_schema,
        )


class TestCheckModelColumnMetaPropagated:
    @pytest.mark.parametrize(
        ("child_meta", "parent_meta", "evolution", "check_fn"),
        [
            pytest.param(
                {"pii": True}, {"pii": True}, "copy", check_passes, id="propagated"
            ),
            pytest.param({}, {}, "copy", check_passes, id="not_classified"),
            pytest.param({}, {"pii": False}, "copy", check_passes, id="falsy_parent"),
            pytest.param(
                {}, {"pii": True}, "scan", check_passes, id="scan_edge_ignored"
            ),
            pytest.param({}, {"pii": True}, "copy", check_fails, id="copy_missing"),
            pytest.param({}, {"pii": True}, "mod", check_fails, id="mod_missing"),
        ],
    )
    def test_propagation(self, child_meta, parent_meta, evolution, check_fn):
        check_fn(
            "check_model_column_meta_propagated",
            model={"unique_id": MODEL},
            meta_key="pii",
            ctx_info_schema=InfoSchema.from_rows(
                column_lineage=[_edge(SOURCE, "email", MODEL, "email", evolution)]
            ),
            ctx_manifest_obj={
                "nodes": {MODEL: _manifest_node(MODEL, {"email": child_meta})},
                "sources": {SOURCE: _manifest_node(SOURCE, {"email": parent_meta})},
            },
        )

    def test_reads_config_meta(self):
        """Dbt 1.10+ nests column `meta` under `config`."""
        child = {
            "unique_id": MODEL,
            "columns": {"email": {"name": "email", "config": {"meta": {"pii": True}}}},
        }
        check_passes(
            "check_model_column_meta_propagated",
            model={"unique_id": MODEL},
            meta_key="pii",
            ctx_info_schema=InfoSchema.from_rows(
                column_lineage=[_edge(SOURCE, "email", MODEL, "email")]
            ),
            ctx_manifest_obj={
                "nodes": {MODEL: child},
                "sources": {SOURCE: _manifest_node(SOURCE, {"email": {"pii": True}})},
            },
        )


class TestCheckModelColumnTypesMatchInferred:
    @pytest.mark.parametrize(
        ("declared", "inferred", "check_fn"),
        [
            pytest.param("bigint", "Int64", check_passes, id="integer"),
            pytest.param("INT64", "Int32", check_passes, id="bigquery_integer"),
            pytest.param("string", "Utf8", check_passes, id="string"),
            pytest.param("varchar(255)", "LargeUtf8", check_passes, id="sized_string"),
            pytest.param(
                "numeric(10, 2)", "Decimal128(10, 2)", check_passes, id="decimal"
            ),
            pytest.param(
                "timestamp_ntz",
                "Timestamp(Microsecond, None)",
                check_passes,
                id="timestamp",
            ),
            pytest.param("date", "Date32", check_passes, id="date"),
            pytest.param("boolean", "Boolean", check_passes, id="boolean"),
            pytest.param(
                "array<string>", "List(Utf8)", check_passes, id="unknown_family"
            ),
            pytest.param("bigint", None, check_passes, id="no_inferred_type"),
            pytest.param(None, "Int64", check_passes, id="no_declared_type"),
            pytest.param("double", "Int64", check_fails, id="float_vs_integer"),
            pytest.param("string", "Int64", check_fails, id="string_vs_integer"),
            pytest.param(
                "date",
                "Timestamp(Microsecond, None)",
                check_fails,
                id="date_vs_timestamp",
            ),
        ],
    )
    def test_types(self, declared, inferred, check_fn):
        info_schema = InfoSchema.from_rows(
            node_columns=[
                _column(
                    MODEL,
                    "amount",
                    data_type_declared=declared,
                    data_type_inferred=inferred,
                )
            ]
        )
        check_fn(
            "check_model_column_types_match_inferred",
            model={"unique_id": MODEL},
            ctx_info_schema=info_schema,
        )


class TestCheckModelColumnsHaveLineage:
    def _info_schema(self):
        return InfoSchema.from_rows(
            column_lineage=[_edge(PARENT, "id", MODEL, "id")],
            node_columns=[_column(MODEL, "id"), _column(MODEL, "channel")],
        )

    def test_column_without_lineage_fails(self):
        check_fails(
            "check_model_columns_have_lineage",
            match="`channel`",
            model={"unique_id": MODEL, "depends_on": {"nodes": [PARENT]}},
            ctx_info_schema=self._info_schema(),
        )

    def test_excluded_column_passes(self):
        check_passes(
            "check_model_columns_have_lineage",
            exclude_column_name_pattern="^channel$",
            model={"unique_id": MODEL, "depends_on": {"nodes": [PARENT]}},
            ctx_info_schema=self._info_schema(),
        )

    def test_model_without_any_lineage_fails(self):
        check_fails(
            "check_model_columns_have_lineage",
            match="static analysis",
            model={"unique_id": MODEL, "depends_on": {"nodes": [PARENT]}},
            ctx_info_schema=InfoSchema.from_rows(node_columns=[_column(MODEL, "id")]),
        )

    def test_ephemeral_model_passes(self):
        check_passes(
            "check_model_columns_have_lineage",
            model={
                "unique_id": MODEL,
                "config": {"materialized": "ephemeral"},
                "depends_on": {"nodes": [PARENT]},
            },
            ctx_info_schema=InfoSchema.from_rows(node_columns=[_column(MODEL, "id")]),
        )

    def test_model_without_parents_passes(self):
        check_passes(
            "check_model_columns_have_lineage",
            model={"unique_id": MODEL, "depends_on": {"nodes": []}},
            ctx_info_schema=InfoSchema.from_rows(node_columns=[_column(MODEL, "id")]),
        )


class TestCheckModelPublicColumnsNotDerivedFromMeta:
    def _run(self, check_fn, access, source_meta, **kwargs):
        # SOURCE.email -> PARENT.email (mod) -> MODEL.contact (copy)
        check_fn(
            "check_model_public_columns_not_derived_from_meta",
            model={"unique_id": MODEL, "access": access},
            meta_key="pii",
            ctx_info_schema=InfoSchema.from_rows(
                column_lineage=[
                    _edge(SOURCE, "email", PARENT, "email", "mod"),
                    _edge(PARENT, "email", MODEL, "contact"),
                ],
                node_columns=[_column(MODEL, "contact")],
            ),
            ctx_manifest_obj={
                "sources": {SOURCE: _manifest_node(SOURCE, {"email": source_meta})}
            },
            **kwargs,
        )

    def test_public_model_with_transitive_pii_fails(self):
        self._run(
            check_fails,
            "public",
            {"pii": True},
            match=r"`contact` \(from `source\.package_name\.source_1\.table_1\.email`\)",
        )

    def test_public_model_without_pii_passes(self):
        self._run(check_passes, "public", {})

    def test_protected_model_passes(self):
        self._run(check_passes, "protected", {"pii": True})

    def test_lineage_cycle_terminates(self):
        check_passes(
            "check_model_public_columns_not_derived_from_meta",
            model={"unique_id": MODEL, "access": "public"},
            meta_key="pii",
            ctx_info_schema=InfoSchema.from_rows(
                column_lineage=[
                    _edge(PARENT, "id", MODEL, "id"),
                    _edge(MODEL, "id", PARENT, "id"),
                ],
                node_columns=[_column(MODEL, "id")],
            ),
        )
