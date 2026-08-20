"""Regression tests for wrapped source metadata in catalog source checks.

In a live runner ``ctx.sources`` holds wrapper objects (a ``SimpleNamespace``
that nests the real source under a ``.source`` attribute), while the unwrapped
sources are exposed via ``ctx.sources_by_unique_id``. The unit-test harness in
``dbt_bouncer.testing`` instead builds ``ctx.sources`` as a flat list and leaves
``sources_by_unique_id`` empty, so the standard ``check_passes`` tests never
exercise the wrapped shape. These tests build the production shape directly to
guard against a regression where ``check_source_columns_are_all_documented``
reads columns off the wrapper and crashes. See GitHub issue #1064.
"""

from types import SimpleNamespace

import pytest

from dbt_bouncer.artifact_parsers.parser import wrap_dict
from dbt_bouncer.check_framework.context import CheckContext
from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError
from dbt_bouncer.runner import _get_resource_meta
from dbt_bouncer.testing import _get_check_class

_UNIQUE_ID = "source.package_name.my_source.my_table"


def _columns(names):
    """Build a catalog/manifest columns mapping keyed by column name.

    Returns:
        dict: A mapping of column name to its column entry.

    """
    return {name: {"name": name} for name in names}


def _catalog_source(names):
    """Build the flat catalog source object the check receives.

    Returns:
        DictProxy: The catalog source with the given column names.

    """
    return wrap_dict({"unique_id": _UNIQUE_ID, "columns": _columns(names)})


def _wrapped_context(documented, adapter_type="duckdb"):
    """Build a CheckContext with the wrapped, production-shape source list.

    ``sources`` holds a wrapper (the real source nested under ``.source``) and
    ``sources_by_unique_id`` holds the unwrapped source, exactly as ``runner``
    assembles them for a live run.

    Returns:
        CheckContext: The context with a wrapped source and its lookup.

    """
    inner = wrap_dict({"unique_id": _UNIQUE_ID, "columns": _columns(documented)})
    wrapper = SimpleNamespace(
        unique_id=_UNIQUE_ID,
        original_file_path="models/staging/_sources.yml",
        source=inner,
    )
    return CheckContext(
        sources=[wrapper],
        sources_by_unique_id={_UNIQUE_ID: inner},
        manifest_obj=SimpleNamespace(
            manifest=wrap_dict({"metadata": {"adapter_type": adapter_type}})
        ),
    )


def _execute(catalog_columns, documented_columns, adapter_type="duckdb"):
    """Run check_source_columns_are_all_documented against the wrapped shape."""
    cls = _get_check_class("check_source_columns_are_all_documented")
    check = cls(
        name="check_source_columns_are_all_documented",
        catalog_source=_catalog_source(catalog_columns),
    )
    check.set_context(_wrapped_context(documented_columns, adapter_type))
    check.execute()


class TestSourceColumnsWrappedShape:
    def test_all_documented_passes(self):
        # The bug crashed here with AttributeError instead of passing.
        _execute(["col_1", "col_2"], ["col_1", "col_2"])

    def test_undocumented_column_fails_with_message(self):
        with pytest.raises(
            DbtBouncerFailedCheckError, match="not included in the sources properties"
        ):
            _execute(["col_1", "col_2"], ["col_1"])

    def test_snowflake_uppercase_columns_pass(self):
        # Snowflake folds identifiers to uppercase in the catalog; the check
        # compares case-insensitively for that adapter.
        _execute(["COL_1", "COL_2"], ["col_1", "col_2"], adapter_type="snowflake")


class TestGetResourceMetaCatalogSource:
    def test_returns_meta_without_crashing(self):
        # The bug fell through to a branch that read `.meta` off the wrapper and
        # crashed with AttributeError during check assembly.
        resource = SimpleNamespace(
            unique_id=_UNIQUE_ID, source=wrap_dict({"unique_id": _UNIQUE_ID})
        )
        meta = {
            "dbt-bouncer": {"skip_checks": ["check_source_columns_are_all_documented"]}
        }
        assert (
            _get_resource_meta(resource, "catalog_source", {_UNIQUE_ID: meta}) == meta
        )

    def test_returns_empty_when_absent(self):
        resource = SimpleNamespace(unique_id=_UNIQUE_ID)
        assert _get_resource_meta(resource, "catalog_source", {}) == {}
