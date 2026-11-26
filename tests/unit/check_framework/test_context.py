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


class TestCheckContextProperties:
    """Property-based tests for lazy CheckContext reverse-lookup indexes."""

    def test_dict_and_namespace_indexes_are_equivalent_and_lazy(self):
        """Lazy indexes match across DictProxy and SimpleNamespace and only build on access."""
        from hypothesis import given, settings
        from hypothesis import strategies as st

        from dbt_bouncer.artifact_parsers.parser import wrap_dict

        node_ids = ["model.pkg.a", "model.pkg.b", "model.pkg.c", "source.pkg.s.t"]
        deps_strategy = st.none() | st.lists(st.sampled_from(node_ids), max_size=4)

        @settings(max_examples=50)
        @given(
            model_deps=st.lists(deps_strategy, max_size=5),
            test_specs=st.lists(
                st.tuples(st.none() | st.sampled_from(node_ids), deps_strategy),
                max_size=5,
            ),
            unit_test_deps=st.lists(deps_strategy, max_size=4),
            source_specs=st.lists(
                st.tuples(
                    st.sampled_from(["dev", "prod"]),
                    st.sampled_from(["raw", "stg"]),
                    st.sampled_from(["t1", "t2"]),
                    st.booleans(),
                ),
                max_size=5,
            ),
        )
        def _check(model_deps, test_specs, unit_test_deps, source_specs):
            ns_models = [
                _node(f"model.pkg.m{i}", depends_on_nodes=deps)
                for i, deps in enumerate(model_deps)
            ]
            dict_models = [
                wrap_dict(
                    {
                        "unique_id": f"model.pkg.m{i}",
                        **(
                            {"depends_on": {"nodes": list(deps)}}
                            if deps is not None
                            else {}
                        ),
                    }
                )
                for i, deps in enumerate(model_deps)
            ]

            ns_tests = [
                _node(
                    f"test.pkg.t{i}",
                    attached_node=attached,
                    depends_on_nodes=deps,
                )
                for i, (attached, deps) in enumerate(test_specs)
            ]
            dict_tests = [
                wrap_dict(
                    {
                        "unique_id": f"test.pkg.t{i}",
                        "attached_node": attached,
                        **(
                            {"depends_on": {"nodes": list(deps)}}
                            if deps is not None
                            else {}
                        ),
                    }
                )
                for i, (attached, deps) in enumerate(test_specs)
            ]

            ns_uts = [
                _node(f"unit_test.pkg.ut{i}", depends_on_nodes=deps)
                for i, deps in enumerate(unit_test_deps)
            ]
            dict_uts = [
                wrap_dict(
                    {
                        "unique_id": f"unit_test.pkg.ut{i}",
                        **(
                            {"depends_on": {"nodes": list(deps)}}
                            if deps is not None
                            else {}
                        ),
                    }
                )
                for i, deps in enumerate(unit_test_deps)
            ]

            ns_sources = []
            dict_sources = []
            for i, (db, schema, ident, wrapped) in enumerate(source_specs):
                uid = f"source.pkg.s{i}.{ident}"
                ns_inner = SimpleNamespace(
                    unique_id=uid, database=db, schema=schema, identifier=ident
                )
                dict_inner = wrap_dict(
                    {
                        "unique_id": uid,
                        "database": db,
                        "schema": schema,
                        "identifier": ident,
                    }
                )
                ns_sources.append(
                    SimpleNamespace(source=ns_inner) if wrapped else ns_inner
                )
                dict_sources.append(
                    SimpleNamespace(source=dict_inner) if wrapped else dict_inner
                )

            ctx_ns = CheckContext(
                models=ns_models,
                sources=ns_sources,
                tests=ns_tests,
                unit_tests=ns_uts,
            )
            ctx_dict = CheckContext(
                models=dict_models,
                sources=dict_sources,
                tests=dict_tests,
                unit_tests=dict_uts,
            )

            # Accessing children_by_unique_id must not eagerly build other indexes.
            assert ctx_dict._children_by_unique_id is None
            children_dict = ctx_dict.children_by_unique_id
            assert ctx_dict._tests_by_attached_node is None
            assert ctx_dict._tests_by_depends_on_node is None
            assert ctx_dict._unit_tests_by_depends_on_node is None
            assert ctx_dict._sources_by_relation is None
            assert ctx_dict.children_by_unique_id is children_dict

            def _to_uids(mapping: dict) -> dict[str, list[str]]:
                return {
                    k: [item.unique_id for item in items]
                    for k, items in mapping.items()
                }

            assert _to_uids(children_dict) == _to_uids(ctx_ns.children_by_unique_id)
            assert _to_uids(ctx_dict.tests_by_attached_node) == _to_uids(
                ctx_ns.tests_by_attached_node
            )
            assert _to_uids(ctx_dict.tests_by_depends_on_node) == _to_uids(
                ctx_ns.tests_by_depends_on_node
            )
            assert _to_uids(ctx_dict.unit_tests_by_depends_on_node) == _to_uids(
                ctx_ns.unit_tests_by_depends_on_node
            )
            assert ctx_dict.sources_by_relation == ctx_ns.sources_by_relation

        _check()
