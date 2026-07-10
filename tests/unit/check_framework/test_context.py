from types import SimpleNamespace

from dbt_bouncer.check_framework.context import CheckContext


def _node(unique_id, *, depends_on_nodes=None, **extra):
    return SimpleNamespace(
        unique_id=unique_id,
        depends_on=SimpleNamespace(nodes=depends_on_nodes)
        if depends_on_nodes is not None
        else None,
        **extra,
    )


class TestChildrenByUniqueId:
    def test_maps_upstream_id_to_downstream_models(self):
        model_1 = _node("model.pkg.model_1")
        model_2 = _node("model.pkg.model_2", depends_on_nodes=["model.pkg.model_1"])
        ctx = CheckContext(models=[model_1, model_2])

        assert ctx.children_by_unique_id.get("model.pkg.model_1") == [model_2]
        assert ctx.children_by_unique_id.get("model.pkg.does_not_exist", []) == []

    def test_duplicate_dependency_in_same_model_not_double_counted(self):
        # A model listing the same upstream id twice in depends_on.nodes must
        # still only contribute one entry to that upstream's children list.
        model_1 = _node("model.pkg.model_1")
        model_2 = _node(
            "model.pkg.model_2",
            depends_on_nodes=["model.pkg.model_1", "model.pkg.model_1"],
        )
        ctx = CheckContext(models=[model_1, model_2])

        assert ctx.children_by_unique_id["model.pkg.model_1"] == [model_2]

    def test_models_with_no_depends_on_are_ignored(self):
        model_1 = _node("model.pkg.model_1")
        ctx = CheckContext(models=[model_1])

        assert ctx.children_by_unique_id == {}


class TestTestsByAttachedNode:
    def test_maps_attached_node_to_tests(self):
        test_1 = _node("test.pkg.test_1", attached_node="model.pkg.model_1")
        test_2 = _node("test.pkg.test_2", attached_node=None)
        ctx = CheckContext(tests=[test_1, test_2])

        assert ctx.tests_by_attached_node.get("model.pkg.model_1") == [test_1]
        assert ctx.tests_by_attached_node.get("model.pkg.does_not_exist", []) == []


class TestTestsByDependsOnNode:
    def test_multi_dependency_test_indexed_under_every_node(self):
        # A relationships test depends on both the owning model and the
        # referenced (target) model - it must show up under both keys.
        relationships_test = _node(
            "test.pkg.relationships_test",
            depends_on_nodes=["model.pkg.model_1", "model.pkg.model_2"],
        )
        ctx = CheckContext(tests=[relationships_test])

        assert ctx.tests_by_depends_on_node["model.pkg.model_1"] == [relationships_test]
        assert ctx.tests_by_depends_on_node["model.pkg.model_2"] == [relationships_test]

    def test_duplicate_node_in_same_test_not_double_counted(self):
        test_1 = _node(
            "test.pkg.test_1",
            depends_on_nodes=[
                "source.pkg.source_1.table_1",
                "source.pkg.source_1.table_1",
            ],
        )
        ctx = CheckContext(tests=[test_1])

        assert ctx.tests_by_depends_on_node["source.pkg.source_1.table_1"] == [test_1]


class TestUnitTestsByDependsOnNode:
    def test_indexed_by_first_node_only(self):
        unit_test = _node(
            "unit_test.pkg.model_1.unit_test_1",
            depends_on_nodes=["model.pkg.model_1", "model.pkg.model_2"],
        )
        ctx = CheckContext(unit_tests=[unit_test])

        assert ctx.unit_tests_by_depends_on_node.get("model.pkg.model_1") == [unit_test]
        assert ctx.unit_tests_by_depends_on_node.get("model.pkg.model_2", []) == []

    def test_unit_test_with_no_depends_on_is_ignored(self):
        unit_test = _node("unit_test.pkg.model_1.unit_test_1")
        ctx = CheckContext(unit_tests=[unit_test])

        assert ctx.unit_tests_by_depends_on_node == {}


class TestSourcesByRelation:
    def test_maps_relation_to_source_unique_ids(self):
        source_1 = SimpleNamespace(
            unique_id="source.pkg.source_1.table_1",
            database="dev",
            schema="raw",
            identifier="table_1",
        )
        source_2_duplicate = SimpleNamespace(
            unique_id="source.pkg.source_2.table_1",
            database="dev",
            schema="raw",
            identifier="table_1",
        )
        ctx = CheckContext(sources=[source_1, source_2_duplicate])

        assert ctx.sources_by_relation["dev", "raw", "table_1"] == [
            "source.pkg.source_1.table_1",
            "source.pkg.source_2.table_1",
        ]

    def test_handles_wrapped_source_objects(self):
        # Production ctx.sources holds SourceWrapper objects (a `.source`
        # attribute wrapping the real SourceNode); unwrap the same way
        # check_duplicate_sources does before reading relation fields.
        inner = SimpleNamespace(
            unique_id="source.pkg.source_1.table_1",
            database="dev",
            schema="raw",
            identifier="table_1",
        )
        wrapped = SimpleNamespace(source=inner)
        ctx = CheckContext(sources=[wrapped])

        assert ctx.sources_by_relation["dev", "raw", "table_1"] == [
            "source.pkg.source_1.table_1",
        ]
