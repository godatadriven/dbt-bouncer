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
