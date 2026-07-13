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
    """Tests for CheckContext.children_by_unique_id."""

    def test_maps_upstream_id_to_downstream_models(self):
        """A model's unique_id maps to the downstream models that depend on it."""
        model_1 = _node("model.pkg.model_1")
        model_2 = _node("model.pkg.model_2", depends_on_nodes=["model.pkg.model_1"])
        ctx = CheckContext(models=[model_1, model_2])

        assert ctx.children_by_unique_id.get("model.pkg.model_1") == [model_2]
        assert ctx.children_by_unique_id.get("model.pkg.does_not_exist", []) == []

    def test_duplicate_dependency_in_same_model_not_double_counted(self):
        """A model listing the same upstream id twice in depends_on.nodes is only counted once."""
        model_1 = _node("model.pkg.model_1")
        model_2 = _node(
            "model.pkg.model_2",
            depends_on_nodes=["model.pkg.model_1", "model.pkg.model_1"],
        )
        ctx = CheckContext(models=[model_1, model_2])

        assert ctx.children_by_unique_id["model.pkg.model_1"] == [model_2]

    def test_models_with_no_depends_on_are_ignored(self):
        """Models with no depends_on contribute no entries to the index."""
        model_1 = _node("model.pkg.model_1")
        ctx = CheckContext(models=[model_1])

        assert ctx.children_by_unique_id == {}


class TestTestsByAttachedNode:
    """Tests for CheckContext.tests_by_attached_node."""

    def test_maps_attached_node_to_tests(self):
        """A test's attached_node maps to the list of tests attached to it."""
        test_1 = _node("test.pkg.test_1", attached_node="model.pkg.model_1")
        test_2 = _node("test.pkg.test_2", attached_node=None)
        ctx = CheckContext(tests=[test_1, test_2])

        assert ctx.tests_by_attached_node.get("model.pkg.model_1") == [test_1]
        assert ctx.tests_by_attached_node.get("model.pkg.does_not_exist", []) == []


class TestTestsByDependsOnNode:
    """Tests for CheckContext.tests_by_depends_on_node."""

    def test_multi_dependency_test_indexed_under_every_node(self):
        """A test depending on multiple nodes (e.g. a relationships test) is indexed under every node."""
        relationships_test = _node(
            "test.pkg.relationships_test",
            depends_on_nodes=["model.pkg.model_1", "model.pkg.model_2"],
        )
        ctx = CheckContext(tests=[relationships_test])

        assert ctx.tests_by_depends_on_node["model.pkg.model_1"] == [relationships_test]
        assert ctx.tests_by_depends_on_node["model.pkg.model_2"] == [relationships_test]

    def test_duplicate_node_in_same_test_not_double_counted(self):
        """A test listing the same node twice in depends_on.nodes is only counted once."""
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
    """Tests for CheckContext.unit_tests_by_depends_on_node."""

    def test_indexed_by_first_node_only(self):
        """A unit test is indexed only under the first node in depends_on.nodes."""
        unit_test = _node(
            "unit_test.pkg.model_1.unit_test_1",
            depends_on_nodes=["model.pkg.model_1", "model.pkg.model_2"],
        )
        ctx = CheckContext(unit_tests=[unit_test])

        assert ctx.unit_tests_by_depends_on_node.get("model.pkg.model_1") == [unit_test]
        assert ctx.unit_tests_by_depends_on_node.get("model.pkg.model_2", []) == []

    def test_unit_test_with_no_depends_on_is_ignored(self):
        """Unit tests with no depends_on contribute no entries to the index."""
        unit_test = _node("unit_test.pkg.model_1.unit_test_1")
        ctx = CheckContext(unit_tests=[unit_test])

        assert ctx.unit_tests_by_depends_on_node == {}


class TestSourcesByRelation:
    """Tests for CheckContext.sources_by_relation."""

    def test_maps_relation_to_source_unique_ids(self):
        """Sources sharing the same (database, schema, identifier) relation are grouped under the same key."""
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
        """Wrapped source objects (a `.source` attribute) are unwrapped before reading relation fields."""
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
