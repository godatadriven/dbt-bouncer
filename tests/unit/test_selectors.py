"""Unit tests for dbt_bouncer.selectors."""

from types import SimpleNamespace

import pytest

from dbt_bouncer.exceptions import DbtBouncerConfigError
from dbt_bouncer.selectors import Selector, parse_selector


def _node(name, tags=None, package="my_project", path=None, fqn=None):
    return SimpleNamespace(
        fqn=fqn or ["my_project", name],
        name=name,
        original_file_path=path or f"models/{name}.sql",
        package_name=package,
        tags=tags or [],
    )


@pytest.fixture
def manifest():
    """Build a small fake manifest: source -> stg_orders -> orders -> exposure.

    Returns:
        SimpleNamespace: The fake manifest.

    """
    return SimpleNamespace(
        child_map={
            "exposure.my_project.dashboard": [],
            "model.my_project.orders": ["exposure.my_project.dashboard"],
            "model.my_project.stg_orders": ["model.my_project.orders"],
            "source.my_project.raw.raw_orders": ["model.my_project.stg_orders"],
        },
        exposures={"exposure.my_project.dashboard": _node("dashboard")},
        macros={},
        nodes={
            "model.my_project.orders": _node(
                "orders",
                tags=["finance"],
                path="models/marts/orders.sql",
                fqn=["my_project", "marts", "orders"],
            ),
            "model.my_project.stg_orders": _node(
                "stg_orders",
                tags=["staging"],
                path="models/staging/stg_orders.sql",
                fqn=["my_project", "staging", "stg_orders"],
            ),
        },
        parent_map={
            "exposure.my_project.dashboard": ["model.my_project.orders"],
            "model.my_project.orders": ["model.my_project.stg_orders"],
            "model.my_project.stg_orders": ["source.my_project.raw.raw_orders"],
            "source.my_project.raw.raw_orders": [],
        },
        semantic_models={},
        sources={
            "source.my_project.raw.raw_orders": _node(
                "raw_orders", path="models/staging/_sources.yml"
            )
        },
        unit_tests={},
    )


class TestParseSelector:
    """Tests for selector parsing."""

    @pytest.mark.parametrize(
        "raw",
        ["", "   ", "+", "tag:", "state:modified", "config.materialized:table"],
    )
    def test_invalid_selectors_raise(self, raw):
        """Empty selectors and unsupported methods are rejected."""
        with pytest.raises(DbtBouncerConfigError):
            parse_selector(raw)

    def test_union_and_intersection_structure(self):
        """Spaces split unions, commas split intersections."""
        groups = parse_selector("tag:a,path:models tag:b")

        assert [len(g) for g in groups] == [2, 1]

    def test_graph_operators(self):
        """Leading and trailing + set the graph flags."""
        atom = parse_selector("+orders+")[0][0]

        assert atom.ancestors is True
        assert atom.descendants is True
        assert atom.method == "name"
        assert atom.value == "orders"

    def test_unbounded_graph_operators_have_no_degree(self):
        """A bare + operator carries no degree limit."""
        atom = parse_selector("+orders+")[0][0]

        assert atom.ancestor_degree is None
        assert atom.descendant_degree is None

    def test_degree_operators(self):
        """Numeric graph operators set the degree limit on each side."""
        atom = parse_selector("2+orders+3")[0][0]

        assert atom.ancestors is True
        assert atom.ancestor_degree == 2
        assert atom.descendants is True
        assert atom.descendant_degree == 3
        assert atom.method == "name"
        assert atom.value == "orders"

    def test_degree_operator_wraps_a_method(self):
        """A degree operator can wrap a method atom."""
        atom = parse_selector("2+tag:critical")[0][0]

        assert atom.ancestors is True
        assert atom.ancestor_degree == 2
        assert atom.method == "tag"
        assert atom.value == "critical"

    def test_at_operator(self):
        """The @ operator sets the at flag and clears graph flags."""
        atom = parse_selector("@orders")[0][0]

        assert atom.at is True
        assert atom.ancestors is False
        assert atom.descendants is False
        assert atom.method == "name"
        assert atom.value == "orders"

    def test_at_operator_wraps_a_method(self):
        """The @ operator can wrap a method atom."""
        atom = parse_selector("@tag:critical")[0][0]

        assert atom.at is True
        assert atom.method == "tag"
        assert atom.value == "critical"


class TestSelectorMatching:
    """Tests for selector resolution against a manifest."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("orders", {"model.my_project.orders"}),
            ("stg_*", {"model.my_project.stg_orders"}),
            ("tag:finance", {"model.my_project.orders"}),
            ("package:my_project", None),  # everything; checked separately
            (
                "path:models/staging",
                {
                    "model.my_project.stg_orders",
                    "source.my_project.raw.raw_orders",
                },
            ),
            ("fqn:my_project.marts.*", {"model.my_project.orders"}),
            (
                "tag:finance tag:staging",
                {"model.my_project.orders", "model.my_project.stg_orders"},
            ),
            ("stg_*,tag:finance", set()),
            ("stg_*,tag:staging", {"model.my_project.stg_orders"}),
            (
                "+orders",
                {
                    "model.my_project.orders",
                    "model.my_project.stg_orders",
                    "source.my_project.raw.raw_orders",
                },
            ),
            (
                "orders+",
                {"model.my_project.orders", "exposure.my_project.dashboard"},
            ),
            (
                "+tag:staging",
                {
                    "model.my_project.stg_orders",
                    "source.my_project.raw.raw_orders",
                },
            ),
        ],
    )
    def test_selection(self, manifest, raw, expected):
        """Each selector resolves to the expected set of unique IDs."""
        selector = Selector(raw, manifest)

        if expected is None:
            assert selector.matches("model.my_project.orders")
            assert selector.matches("model.my_project.stg_orders")
        else:
            all_ids = (
                set(manifest.nodes) | set(manifest.sources) | set(manifest.exposures)
            )
            assert {uid for uid in all_ids if selector.matches(uid)} == expected

    def test_none_unique_id_never_matches(self, manifest):
        """A resource without a unique ID is never selected."""
        selector = Selector("orders", manifest)

        assert selector.matches(None) is False

    def test_tag_atom_ignores_resources_without_tags(self, manifest):
        """A tag atom does not match resources lacking a tags attribute."""
        manifest.macros["macro.my_project.my_macro"] = SimpleNamespace(name="my_macro")
        selector = Selector("tag:finance", manifest)

        assert selector.matches("macro.my_project.my_macro") is False


class TestDegreeLimits:
    """Tests for numeric degree limits on graph operators."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            # One hop of ancestors stops at stg_orders.
            ("1+orders", {"model.my_project.orders", "model.my_project.stg_orders"}),
            # Two hops reach the source, i.e. the full chain here.
            (
                "2+orders",
                {
                    "model.my_project.orders",
                    "model.my_project.stg_orders",
                    "source.my_project.raw.raw_orders",
                },
            ),
            # A degree larger than the graph behaves like the unbounded form.
            (
                "9+orders",
                {
                    "model.my_project.orders",
                    "model.my_project.stg_orders",
                    "source.my_project.raw.raw_orders",
                },
            ),
            # One hop each side excludes the second-degree descendant (dashboard).
            (
                "1+stg_orders+1",
                {
                    "source.my_project.raw.raw_orders",
                    "model.my_project.stg_orders",
                    "model.my_project.orders",
                },
            ),
        ],
    )
    def test_degree_selection(self, manifest, raw, expected):
        """A degree limit truncates the graph walk at the given number of hops."""
        selector = Selector(raw, manifest)
        all_ids = set(manifest.nodes) | set(manifest.sources) | set(manifest.exposures)

        assert {uid for uid in all_ids if selector.matches(uid)} == expected


@pytest.fixture
def diamond_manifest():
    """Build a manifest with a diamond so @ differs from +x+.

    Edges: a -> b, a -> c, b -> d, c -> d, c -> e.

    Returns:
        SimpleNamespace: The fake manifest.

    """
    ids = {k: f"model.my_project.{k}" for k in "abcde"}
    return SimpleNamespace(
        child_map={
            ids["a"]: [ids["b"], ids["c"]],
            ids["b"]: [ids["d"]],
            ids["c"]: [ids["d"], ids["e"]],
            ids["d"]: [],
            ids["e"]: [],
        },
        exposures={},
        macros={},
        nodes={ids[k]: _node(k) for k in "abcde"},
        parent_map={
            ids["a"]: [],
            ids["b"]: [ids["a"]],
            ids["c"]: [ids["a"]],
            ids["d"]: [ids["b"], ids["c"]],
            ids["e"]: [ids["c"]],
        },
        semantic_models={},
        sources={},
        unit_tests={},
    )


class TestAtOperator:
    """Tests for the @ operator."""

    def test_at_includes_parents_of_descendants(self, diamond_manifest):
        """@b selects b, its descendants, and the ancestors of those descendants."""
        selector = Selector("@b", diamond_manifest)
        all_ids = set(diamond_manifest.nodes)

        # b, d (descendant), a and c (parents of b and d). e is excluded.
        assert {uid for uid in all_ids if selector.matches(uid)} == {
            "model.my_project.a",
            "model.my_project.b",
            "model.my_project.c",
            "model.my_project.d",
        }

    def test_at_differs_from_both_sided_plus(self, diamond_manifest):
        """+b+ omits c, which @b includes."""
        both_sided = Selector("+b+", diamond_manifest)
        all_ids = set(diamond_manifest.nodes)

        # +b+ = ancestors(b)={a} + descendants(b)={d} + b. No c.
        assert {uid for uid in all_ids if both_sided.matches(uid)} == {
            "model.my_project.a",
            "model.my_project.b",
            "model.my_project.d",
        }


def test_invalid_selector_rejected_at_config_time():
    """A syntactically invalid selector fails config validation."""
    from dbt_bouncer.configuration_file.validator import validate_conf

    with pytest.raises(DbtBouncerConfigError):
        validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents={
                "manifest_checks": [
                    {
                        "name": "check_model_description_populated",
                        "selector": "state:modified",
                    }
                ]
            },
        )


def test_invalid_global_selector_rejected_at_config_time():
    """A syntactically invalid global selector fails config validation."""
    from dbt_bouncer.configuration_file.validator import validate_conf

    with pytest.raises(DbtBouncerConfigError):
        validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents={
                "manifest_checks": [{"name": "check_model_description_populated"}],
                "selector": "state:modified",
            },
        )
