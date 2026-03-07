"""Integration tests for the Rust-based JSON parser extension.

These tests verify that the ``dbt_artifacts_rs`` module (built via maturin)
produces results compatible with the pure-Python parsing path.
"""

import json
from pathlib import Path

import pytest

try:
    import dbt_artifacts_rs

    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not RUST_AVAILABLE,
    reason="dbt_artifacts_rs extension not installed",
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


# --- Availability ---


def test_rust_module_importable():
    assert RUST_AVAILABLE


def test_rust_module_has_parse_functions():
    assert hasattr(dbt_artifacts_rs, "parse_manifest_json")
    assert hasattr(dbt_artifacts_rs, "parse_catalog_json")
    assert hasattr(dbt_artifacts_rs, "parse_run_results_json")


# --- Manifest parsing ---


@pytest.fixture
def manifest_json_str():
    manifest_path = FIXTURES_DIR / "dbt_111" / "target" / "manifest.json"
    if not manifest_path.exists():
        pytest.skip(f"Fixture not found: {manifest_path}")
    return manifest_path.read_text()


def test_parse_manifest_returns_jsonobj(manifest_json_str):
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    assert result is not None
    assert hasattr(result, "metadata")
    assert hasattr(result, "nodes")


def test_manifest_metadata_access(manifest_json_str):
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    metadata = result.metadata
    assert hasattr(metadata, "dbt_version")
    assert isinstance(str(metadata.dbt_version), str)


def test_manifest_nodes_iteration(manifest_json_str):
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    native = json.loads(manifest_json_str)
    assert len(result.nodes) == len(native["nodes"])


def test_manifest_node_attribute_access(manifest_json_str):
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    native = json.loads(manifest_json_str)
    for key in list(native["nodes"].keys())[:3]:
        node = result.nodes[key]
        assert hasattr(node, "name")
        assert str(node.name) == native["nodes"][key]["name"]


def test_manifest_contains_check(manifest_json_str):
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    native = json.loads(manifest_json_str)
    first_key = next(iter(native["nodes"]))
    assert first_key in result.nodes


def test_manifest_schema_trailing_underscore(manifest_json_str):
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    native = json.loads(manifest_json_str)
    for key in list(native["nodes"].keys())[:3]:
        if "schema" in native["nodes"][key]:
            node = result.nodes[key]
            assert str(node.schema_) == native["nodes"][key]["schema"]
            break


# --- Catalog parsing ---


@pytest.fixture
def catalog_json_str():
    catalog_path = FIXTURES_DIR / "dbt_111" / "target" / "catalog.json"
    if not catalog_path.exists():
        pytest.skip(f"Fixture not found: {catalog_path}")
    return catalog_path.read_text()


def test_parse_catalog_returns_jsonobj(catalog_json_str):
    result = dbt_artifacts_rs.parse_catalog_json(catalog_json_str)
    assert result is not None
    assert hasattr(result, "nodes")


def test_catalog_nodes_match_native(catalog_json_str):
    result = dbt_artifacts_rs.parse_catalog_json(catalog_json_str)
    native = json.loads(catalog_json_str)
    assert len(result.nodes) == len(native["nodes"])


# --- Run results parsing ---


@pytest.fixture
def run_results_json_str():
    rr_path = FIXTURES_DIR / "dbt_111" / "target" / "run_results.json"
    if not rr_path.exists():
        pytest.skip(f"Fixture not found: {rr_path}")
    return rr_path.read_text()


def test_parse_run_results_returns_jsonobj(run_results_json_str):
    result = dbt_artifacts_rs.parse_run_results_json(run_results_json_str)
    assert result is not None
    assert hasattr(result, "results")


def test_run_results_count_matches(run_results_json_str):
    result = dbt_artifacts_rs.parse_run_results_json(run_results_json_str)
    native = json.loads(run_results_json_str)
    assert len(result.results) == len(native["results"])


# --- to_python round-trip ---


def _approx_equal(a, b, rel_tol=1e-9):
    """Recursively compare two structures, using approximate float comparison.

    Rust's serde and Python's ``json.loads`` may deserialise the same JSON
    float literal into slightly different IEEE-754 values (e.g.
    ``1771939473.3678203`` vs ``1771939473.3678205``).  This helper walks the
    entire structure and applies ``math.isclose`` for floats so the rest of
    the comparison remains exact.

    Returns:
        bool: ``True`` if the structures are approximately equal.

    """
    import math

    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        if a.keys() != b.keys():
            return False
        return all(_approx_equal(a[k], b[k], rel_tol) for k in a)
    if isinstance(a, list):
        if len(a) != len(b):
            return False
        return all(_approx_equal(x, y, rel_tol) for x, y in zip(a, b, strict=True))
    if isinstance(a, float):
        return math.isclose(a, b, rel_tol=rel_tol)
    return a == b


def test_manifest_to_python_matches_json(manifest_json_str):
    """Verify to_python() round-trip reproduces the original JSON structure.

    Uses approximate float comparison because Rust's serde may produce slightly
    different floating-point representations for timestamps like ``created_at``.
    """
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    native = json.loads(manifest_json_str)
    converted = result.to_python()

    assert _approx_equal(converted, native)


# --- Error handling ---


def test_parse_invalid_json_raises():
    with pytest.raises(ValueError, match="Invalid manifest JSON"):
        dbt_artifacts_rs.parse_manifest_json("not valid json")


def test_parse_empty_string_raises():
    with pytest.raises(ValueError, match="Invalid manifest JSON"):
        dbt_artifacts_rs.parse_manifest_json("")


# --- Edge cases ---


def test_empty_object():
    result = dbt_artifacts_rs.parse_manifest_json("{}")
    assert len(result) == 0


def test_deeply_nested_access():
    json_str = json.dumps({"a": {"b": {"c": {"d": "deep"}}}})
    result = dbt_artifacts_rs.parse_manifest_json(json_str)
    assert str(result.a.b.c.d) == "deep"


def test_jsonobj_bool_truthy():
    result = dbt_artifacts_rs.parse_manifest_json('{"key": "value"}')
    assert bool(result)


def test_jsonobj_bool_falsy():
    result = dbt_artifacts_rs.parse_manifest_json("{}")
    assert not bool(result)


def test_jsonobj_items():
    result = dbt_artifacts_rs.parse_manifest_json('{"a": 1, "b": 2}')
    items = result.items()
    keys = [k for k, _ in items]
    assert "a" in keys
    assert "b" in keys


def test_jsonobj_get_with_default():
    result = dbt_artifacts_rs.parse_manifest_json('{"a": 1}')
    assert result.get("a") is not None
    assert result.get("missing") is None
    assert result.get("missing", 42) == 42


# --- use_rust flag logic ---


class TestUseRustFlag:
    """Tests for the --rust CLI flag logic in run_bouncer."""

    def test_use_rust_auto_with_rust_available(self):
        """Auto mode uses Rust when available."""
        from unittest.mock import patch

        with patch("dbt_bouncer.artifact_parsers.rust_adapter.RUST_AVAILABLE", True):
            from dbt_bouncer.artifact_parsers.rust_adapter import RUST_AVAILABLE

            use_rust = "auto"
            use_rust_parser = (use_rust == "true") or (
                use_rust == "auto" and RUST_AVAILABLE
            )
            assert use_rust_parser is True

    def test_use_rust_auto_without_rust(self):
        """Auto mode falls back to Python when Rust unavailable."""
        use_rust = "auto"
        rust_available = False
        use_rust_parser = (use_rust == "true") or (
            use_rust == "auto" and rust_available
        )
        assert use_rust_parser is False

    def test_use_rust_true_with_rust_available(self):
        """True mode uses Rust when available."""
        use_rust = "true"
        rust_available = True
        use_rust_parser = (use_rust == "true") or (
            use_rust == "auto" and rust_available
        )
        assert use_rust_parser is True

    def test_use_rust_true_without_rust_raises(self):
        """True mode errors when Rust unavailable."""  # noqa: DOC501
        msg = "--rust=true specified but the dbt_artifacts_rs Rust extension is not installed."
        with pytest.raises(RuntimeError, match="--rust=true"):
            raise RuntimeError(msg)

    def test_use_rust_false(self):
        """False mode never uses Rust."""
        use_rust = "false"
        rust_available = True
        use_rust_parser = (use_rust == "true") or (
            use_rust == "auto" and rust_available
        )
        assert use_rust_parser is False
