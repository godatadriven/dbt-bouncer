"""Test helpers for dbt-bouncer checks.

Provides ``check_passes`` and ``check_fails`` functions that dramatically
reduce test boilerplate. Instead of wiring up fixtures, ``indirect`` parametrize,
and context managers, tests become one-liners::

    from dbt_bouncer.testing import check_fails, check_passes

    def test_pass():
        check_passes("check_model_description_populated",
                      model={"description": "A good description."})

    def test_fail():
        check_fails("check_model_description_populated",
                     model={"description": ""})

Default resource dicts are provided for every resource type and are merged
with caller overrides, so only the fields relevant to the test need to be
specified.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from dbt_bouncer.artifact_parsers.parser import wrap_dict
from dbt_bouncer.check_context import CheckContext
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

# ---------------------------------------------------------------------------
# Default resource factories
# ---------------------------------------------------------------------------

_DEFAULT_CATALOG_NODE: dict[str, Any] = {
    "columns": {
        "col_1": {
            "index": 1,
            "name": "col_1",
            "type": "INTEGER",
        },
        "col_2": {
            "index": 2,
            "name": "col_2",
            "type": "INTEGER",
        },
    },
    "metadata": {
        "name": "table_1",
        "schema": "main",
        "type": "VIEW",
    },
    "stats": {},
    "unique_id": "model.package_name.model_1",
}

_DEFAULT_CATALOG_SOURCE: dict[str, Any] = {
    "columns": {
        "col_1": {
            "index": 1,
            "name": "col_1",
            "type": "INTEGER",
        },
    },
    "metadata": {
        "name": "source_table_1",
        "schema": "main",
        "type": "BASE TABLE",
    },
    "stats": {},
    "unique_id": "source.package_name.source_1.table_1",
}

_DEFAULT_EXPOSURE: dict[str, Any] = {
    "depends_on": {"nodes": ["model.package_name.model_1"]},
    "fqn": ["package_name", "marts", "finance", "exposure_1"],
    "name": "exposure_1",
    "original_file_path": "models/marts/finance/_exposures.yml",
    "owner": {
        "email": "anna.anderson@example.com",
        "name": "Anna Anderson",
    },
    "package_name": "package_name",
    "path": "marts/finance/_exposures.yml",
    "resource_type": "exposure",
    "type": "dashboard",
    "unique_id": "exposure.package_name.exposure_1",
}

_DEFAULT_MACRO: dict[str, Any] = {
    "arguments": [],
    "macro_sql": "select 1",
    "name": "macro_1",
    "original_file_path": "macros/macro_1.sql",
    "package_name": "package_name",
    "path": "macros/macro_1.sql",
    "resource_type": "macro",
    "unique_id": "macro.package_name.macro_1",
}

_DEFAULT_MANIFEST: dict[str, Any] = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
        "dbt_version": "1.11.0a1",
        "project_name": "dbt_bouncer_test_project",
        "adapter_type": "postgres",
    },
    "nodes": {},
    "sources": {},
    "macros": {},
    "docs": {},
    "exposures": {},
    "metrics": {},
    "groups": {},
    "selectors": {},
    "disabled": {},
    "parent_map": {},
    "child_map": {},
    "group_map": {},
    "saved_queries": {},
    "semantic_models": {},
    "unit_tests": {},
    "functions": None,
}

_DEFAULT_MODEL: dict[str, Any] = {
    "alias": "model_1",
    "checksum": {"name": "sha256", "checksum": ""},
    "columns": {
        "col_1": {
            "index": 1,
            "name": "col_1",
            "type": "INTEGER",
        },
    },
    "fqn": ["package_name", "model_1"],
    "language": "sql",
    "name": "model_1",
    "original_file_path": "model_1.sql",
    "package_name": "package_name",
    "path": "staging/finance/model_1.sql",
    "resource_type": "model",
    "schema": "main",
    "unique_id": "model.package_name.model_1",
}

_DEFAULT_RUN_RESULT: dict[str, Any] = {
    "adapter_response": {"bytes_billed": 1},
    "execution_time": 1,
    "status": "success",
    "thread_id": "Thread-1",
    "timing": [],
    "unique_id": "model.package_name.model_1",
}

_DEFAULT_SEED: dict[str, Any] = {
    "alias": "seed_1",
    "checksum": {"name": "sha256", "checksum": ""},
    "columns": {},
    "fqn": ["package_name", "seed_1"],
    "name": "seed_1",
    "original_file_path": "seeds/seed_1.csv",
    "package_name": "package_name",
    "path": "seed_1.csv",
    "resource_type": "seed",
    "schema": "main",
    "unique_id": "seed.package_name.seed_1",
}

_DEFAULT_SEMANTIC_MODEL: dict[str, Any] = {
    "depends_on": {"nodes": ["model.package_name.model_1"]},
    "fqn": ["package_name", "semantic_model_1"],
    "name": "semantic_model_1",
    "original_file_path": "models/semantic_model_1.yml",
    "package_name": "package_name",
    "path": "semantic_model_1.yml",
    "resource_type": "semantic_model",
    "unique_id": "semantic_model.package_name.semantic_model_1",
}

_DEFAULT_SNAPSHOT: dict[str, Any] = {
    "alias": "snapshot_1",
    "checksum": {"name": "sha256", "checksum": ""},
    "columns": {},
    "fqn": ["package_name", "snapshot_1"],
    "name": "snapshot_1",
    "original_file_path": "snapshots/snapshot_1.sql",
    "package_name": "package_name",
    "path": "snapshot_1.sql",
    "resource_type": "snapshot",
    "schema": "main",
    "unique_id": "snapshot.package_name.snapshot_1",
}

_DEFAULT_SOURCE: dict[str, Any] = {
    "fqn": ["package_name", "source_1", "table_1"],
    "identifier": "table_1",
    "loader": "csv",
    "name": "table_1",
    "original_file_path": "models/staging/_sources.yml",
    "package_name": "package_name",
    "path": "models/staging/_sources.yml",
    "resource_type": "source",
    "schema": "main",
    "source_name": "source_1",
    "unique_id": "source.package_name.source_1.table_1",
}

_DEFAULT_TEST: dict[str, Any] = {
    "alias": "not_null_model_1_unique",
    "attached_node": "model.package_name.model_1",
    "checksum": {"name": "none", "checksum": ""},
    "column_name": "col_1",
    "fqn": [
        "package_name",
        "marts",
        "finance",
        "not_null_model_1_unique",
    ],
    "name": "not_null_model_1_unique",
    "original_file_path": "models/marts/finance/_finance__models.yml",
    "package_name": "package_name",
    "path": "not_null_model_1_unique.sql",
    "resource_type": "test",
    "schema": "main",
    "test_metadata": {
        "name": "unique",
    },
    "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
}

_DEFAULT_UNIT_TEST: dict[str, Any] = {
    "depends_on": {
        "nodes": [
            "model.package_name.model_1",
        ],
    },
    "expect": {"format": "dict", "rows": [{"id": 1}]},
    "fqn": [
        "package_name",
        "staging",
        "crm",
        "model_1",
        "unit_test_1",
    ],
    "given": [{"input": "ref(input_1)", "format": "csv"}],
    "model": "model_1",
    "name": "unit_test_1",
    "original_file_path": "models/staging/crm/_crm__source.yml",
    "resource_type": "unit_test",
    "package_name": "package_name",
    "path": "staging/crm/_crm__source.yml",
    "unique_id": "unit_test.package_name.model_1.unit_test_1",
}


# Mapping from resource field name to its default dict.
_RESOURCE_DEFAULTS: dict[str, dict[str, Any]] = {
    "catalog_node": _DEFAULT_CATALOG_NODE,
    "catalog_source": _DEFAULT_CATALOG_SOURCE,
    "exposure": _DEFAULT_EXPOSURE,
    "macro": _DEFAULT_MACRO,
    "model": _DEFAULT_MODEL,
    "run_result": _DEFAULT_RUN_RESULT,
    "seed": _DEFAULT_SEED,
    "semantic_model": _DEFAULT_SEMANTIC_MODEL,
    "snapshot": _DEFAULT_SNAPSHOT,
    "source": _DEFAULT_SOURCE,
    "test": _DEFAULT_TEST,
    "unit_test": _DEFAULT_UNIT_TEST,
}

# Resource types that the runner iterates over.
_RESOURCE_FIELDS = frozenset(_RESOURCE_DEFAULTS.keys())

# Context list fields (ctx_models → ctx.models, etc.)
_CTX_LIST_FIELDS = frozenset(
    f.name
    for f in CheckContext.__dataclass_fields__.values()
    if f.name != "manifest_obj"
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_check_class(name: str) -> type:
    """Look up a check class by its config name.

    Args:
        name: The check name (e.g. ``"check_model_names"``).

    Returns:
        The check class.

    Raises:
        KeyError: If no check with the given name is registered.

    """
    from dbt_bouncer.utils import get_check_registry

    registry = get_check_registry()
    if name not in registry:
        available = sorted(registry.keys())
        raise KeyError(
            f"Unknown check name {name!r}. Available checks: {available[:10]}..."
        )
    return registry[name]


def _build_resource(resource_type: str, overrides: dict[str, Any]) -> Any:
    """Build a wrapped resource by merging defaults with overrides.

    Args:
        resource_type: The resource field name (e.g. ``"model"``).
        overrides: Dict of fields to override on the default resource.

    Returns:
        A ``DictProxy``-wrapped resource object.

    """
    defaults = _RESOURCE_DEFAULTS.get(resource_type, {})
    merged = {**defaults, **overrides}
    return wrap_dict(merged)


def _build_resource_list(resource_type: str, items: list[dict[str, Any]]) -> list[Any]:
    """Build a list of wrapped resources.

    Args:
        resource_type: The resource field name (e.g. ``"model"``).
        items: List of override dicts for each resource.

    Returns:
        List of wrapped resource objects.

    """
    defaults = _RESOURCE_DEFAULTS.get(resource_type, {})
    return [wrap_dict({**defaults, **item}) for item in items]


def _build_manifest_obj(overrides: dict[str, Any] | None = None) -> SimpleNamespace:
    """Build a minimal manifest_obj.

    Args:
        overrides: Dict to merge into the default manifest.

    Returns:
        A ``SimpleNamespace`` wrapping a ``DictProxy`` manifest.

    """
    manifest_data = {**_DEFAULT_MANIFEST, **(overrides or {})}
    return SimpleNamespace(manifest=wrap_dict(manifest_data))


def _build_check_context(**kwargs: Any) -> CheckContext:
    """Build a ``CheckContext`` from keyword arguments.

    Accepts ``ctx_<field>`` kwargs where ``<field>`` matches a ``CheckContext``
    field name. List fields whose names match a resource type are automatically
    wrapped.

    Also accepts ``manifest_obj`` as a dict (auto-wrapped) or pre-built
    ``SimpleNamespace``.

    Args:
        **kwargs: Context field overrides prefixed with ``ctx_``.

    Returns:
        A ``CheckContext`` instance.

    """
    ctx_kwargs: dict[str, Any] = {}

    for key, value in kwargs.items():
        if not key.startswith("ctx_"):
            continue
        field_name = key[4:]  # Strip "ctx_" prefix

        if field_name == "manifest_obj":
            if isinstance(value, dict):
                ctx_kwargs["manifest_obj"] = _build_manifest_obj(value)
            else:
                ctx_kwargs["manifest_obj"] = value
        elif isinstance(value, list):
            # Determine the singular resource type for wrapping.
            singular = (
                field_name.rstrip("s") if field_name.endswith("s") else field_name
            )
            if singular in _RESOURCE_DEFAULTS:
                ctx_kwargs[field_name] = _build_resource_list(singular, value)
            else:
                ctx_kwargs[field_name] = value
        else:
            ctx_kwargs[field_name] = value

    # Provide a default manifest_obj if not specified.
    if "manifest_obj" not in ctx_kwargs:
        ctx_kwargs["manifest_obj"] = _build_manifest_obj()

    return CheckContext(**ctx_kwargs)


def _run_check(name: str, **kwargs: Any) -> None:
    """Instantiate and execute a check.

    Resource kwargs (model, source, etc.) are auto-wrapped from dicts.
    Context kwargs (ctx_models, ctx_sources, etc.) build the CheckContext.
    All other kwargs are passed as check parameters.

    Args:
        name: The check config name.
        **kwargs: Resource overrides, context overrides, and check parameters.

    """
    cls = _get_check_class(name)

    # Separate kwargs into resources, context, and params.
    resource_kwargs: dict[str, Any] = {}
    ctx_kwargs: dict[str, Any] = {}
    param_kwargs: dict[str, Any] = {}

    for key, value in kwargs.items():
        if key.startswith("ctx_"):
            ctx_kwargs[key] = value
        elif key in _RESOURCE_FIELDS:
            if isinstance(value, dict):
                resource_kwargs[key] = _build_resource(key, value)
            else:
                resource_kwargs[key] = value
        else:
            param_kwargs[key] = value

    # Build context.
    ctx = _build_check_context(**ctx_kwargs)

    # Instantiate the check.
    check = cls(name=name, **resource_kwargs, **param_kwargs)
    check._ctx = ctx
    check.execute()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def check_passes(name: str, **kwargs: Any) -> None:
    """Assert that a check passes (does not raise).

    Args:
        name: The check config name (e.g. ``"check_model_names"``).
        **kwargs: Resource overrides (``model={...}``), context overrides
            (``ctx_models=[{...}]``), and check parameters
            (``model_name_pattern="^stg_"``).

    Example::

        check_passes("check_model_names",
                      model={"name": "stg_orders"},
                      model_name_pattern="^stg_")

    """
    try:
        _run_check(name, **kwargs)
    except DbtBouncerFailedCheckError as exc:
        pytest.fail(f"Check {name!r} unexpectedly failed: {exc}")


def check_fails(name: str, **kwargs: Any) -> None:
    """Assert that a check fails with ``DbtBouncerFailedCheckError``.

    Args:
        name: The check config name (e.g. ``"check_model_names"``).
        **kwargs: Resource overrides, context overrides, and check parameters.

    Example::

        check_fails("check_model_names",
                     model={"name": "fct_orders"},
                     model_name_pattern="^stg_")

    """
    try:
        _run_check(name, **kwargs)
    except DbtBouncerFailedCheckError:
        return  # Expected failure.

    pytest.fail(f"Check {name!r} did not fail as expected.")
