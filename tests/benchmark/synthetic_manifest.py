"""Generate a large synthetic dbt manifest for performance benchmarking.

dbt-bouncer's parser (``dbt_bouncer.artifact_parsers.parser``) reads raw JSON
dicts and never schema-validates them, so a "valid" manifest is simply one whose
enabled checks run without raising. This module builds a deterministic manifest
(``~5000`` models by default) plus matching ``catalog.json`` / ``run_results.json``
so the full pipeline — parse, check-assembly, and execution — can be benchmarked
at realistic scale.

The per-resource shapes are seeded from the ``_DEFAULT_*`` templates in
``dbt_bouncer.testing`` (the authoritative minimum-viable structures used by the
unit-test helpers) and then enriched with the fields the checks actually read
(columns, config, depends_on, contract, ...) so checks do real work rather than
short-circuiting on ``None``.

Generation is fully deterministic (index-based, no randomness) so timings and
diffs are reproducible.

Run as a script to materialise the artifacts on disk::

    uv run python tests/benchmark/synthetic_manifest.py --out-dir ./_syn/target --models 5000
"""

from __future__ import annotations

import os
from pathlib import Path  # noqa: TC003 - needed at runtime by Typer
from typing import Annotated, Any

import orjson
import typer

from dbt_bouncer.testing import (
    _DEFAULT_CATALOG_NODE,
    _DEFAULT_CATALOG_SOURCE,
    _DEFAULT_EXPOSURE,
    _DEFAULT_MACRO,
    _DEFAULT_MANIFEST,
    _DEFAULT_MODEL,
    _DEFAULT_RUN_RESULT,
    _DEFAULT_SEED,
    _DEFAULT_SEMANTIC_MODEL,
    _DEFAULT_SNAPSHOT,
    _DEFAULT_SOURCE,
    _DEFAULT_TEST,
    _DEFAULT_UNIT_TEST,
)

DEFAULT_PACKAGE_NAME = "dbt_bouncer_perf"
# >= 1.8.0 so unit_tests are parsed; v12 schema shape.
DEFAULT_DBT_VERSION = "1.11.0"

# Column data types cycled through so column-type checks have real variety.
_COLUMN_TYPES = ["INTEGER", "VARCHAR", "BOOLEAN", "DATE", "DOUBLE", "BIGINT"]


def _env_int(var_name: str, default: int) -> int:
    """Return a positive int from an env var, falling back to ``default``.

    Args:
        var_name: The environment variable to read.
        default: Fallback when the var is unset or unparseable.

    Returns:
        The parsed value floored at 1, or ``default`` unchanged when the var is
        unset or unparseable (no floor is applied to ``default``).
    """
    raw = os.getenv(var_name)
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _env_model_count(default: int) -> int:
    """Return the model count, overridable via ``DBT_BOUNCER_BENCH_MODELS``.

    Args:
        default: Fallback count when the env var is unset or unparseable.

    Returns:
        The resolved model count.
    """
    return _env_int("DBT_BOUNCER_BENCH_MODELS", default)


def _columns(n: int) -> dict[str, dict[str, Any]]:
    """Build ``n`` documented columns with cycling data types."""
    cols: dict[str, dict[str, Any]] = {}
    for i in range(n):
        name = f"col_{i}"
        cols[name] = {
            "index": i + 1,
            "name": name,
            "description": f"Column {i} description.",
            "data_type": _COLUMN_TYPES[i % len(_COLUMN_TYPES)],
            "meta": {"owner": "data-team"},
            "tags": [],
            "constraints": [],
        }
    return cols


def _model_node(idx: int, layer: str, pkg: str, parents: list[str]) -> dict[str, Any]:
    """Build a single model node for the given layer.

    Args:
        idx: Global model index (unique across layers).
        layer: One of ``"staging"``, ``"intermediate"``, ``"marts"``.
        pkg: Package name shared by every resource.
        parents: ``depends_on.nodes`` unique_ids (upstream models/sources).

    Returns:
        A raw model-node dict.
    """
    if layer == "staging":
        sub = "crm" if idx % 2 == 0 else "payments"
        name = f"stg_{sub}_{idx}"
        path = f"staging/{sub}/{name}.sql"
        original_file_path = f"models/staging/{sub}/{name}.sql"
        fqn = [pkg, "staging", sub, name]
        materialized = "view"
        schema = f"stg_{sub}"
        access = "protected"
        enforced = False
        constraints: list[dict[str, Any]] = []
    elif layer == "intermediate":
        name = f"int_{idx}"
        path = f"intermediate/{name}.sql"
        original_file_path = f"models/intermediate/{name}.sql"
        fqn = [pkg, "intermediate", name]
        materialized = "ephemeral"
        schema = "intermediate"
        access = "protected"
        enforced = False
        constraints = []
    else:  # marts
        prefix = "fct" if idx % 2 == 0 else "dim"
        name = f"{prefix}_{idx}"
        path = f"marts/finance/{name}.sql"
        original_file_path = f"models/marts/finance/{name}.sql"
        fqn = [pkg, "marts", "finance", name]
        materialized = "table"
        schema = "marts"
        access = "public"
        enforced = True
        constraints = [{"type": "primary_key", "columns": ["col_0"]}]

    unique_id = f"model.{pkg}.{name}"
    columns = _columns(3)
    if enforced:
        for col in columns.values():
            col["constraints"] = [{"type": "not_null"}]

    node = {
        **_DEFAULT_MODEL,
        "alias": name,
        "name": name,
        "unique_id": unique_id,
        "package_name": pkg,
        "path": path,
        "original_file_path": original_file_path,
        "fqn": fqn,
        "schema": schema,
        "database": "warehouse",
        "columns": columns,
        "description": f"Model {name} in the {layer} layer.",
        "access": access,
        "language": "sql",
        # The ``if parents`` guard makes ``parents[0]`` safe: every model layer
        # is built with a non-empty parent list (staging gets a source parent,
        # intermediate/marts get model parents); the ``else`` is a defensive
        # fallback for a hypothetical parentless model.
        "raw_code": f"select * from {{{{ ref('{parents[0]}') }}}}"  # noqa: S608 - synthetic dbt SQL, not a real query
        if parents
        else "select 1 as col_0;",
        "compiled_code": "select 1 as col_0;",
        "meta": {"maturity": "high", "owner": "data-team"},
        "tags": ["crm"] if layer == "staging" else [],
        "config": {
            "materialized": materialized,
            "enabled": True,
            "meta": {"maturity": "high", "owner": "data-team"},
            "tags": ["crm"] if layer == "staging" else [],
            "access": access,
            "grants": {"select": ["reporter"]},
            "contract": {"enforced": enforced},
        },
        "constraints": constraints,
        "contract": {"enforced": enforced, "alias_types": True},
        "depends_on": {"macros": [f"macro.{pkg}.macro_0"], "nodes": list(parents)},
        "refs": [],
        "sources": [],
        "patch_path": f"{pkg}://models/{layer}/_{layer}__models.yml",
        "latest_version": None,
        "version": None,
    }
    return node


def _test_node(idx: int, model_uid: str, model_name: str, pkg: str, kind: str) -> dict:
    """Build a schema-test node attached to ``model_uid``."""
    name = f"{kind}_{model_name}_col_0"
    unique_id = f"test.{pkg}.{name}.{idx:08x}"
    return {
        **_DEFAULT_TEST,
        "name": name,
        "alias": name,
        "unique_id": unique_id,
        "package_name": pkg,
        "attached_node": model_uid,
        "column_name": "col_0",
        "original_file_path": "models/staging/crm/_crm__models.yml",
        "path": f"{name}.sql",
        "fqn": [pkg, "staging", "crm", name],
        "test_metadata": {"name": kind},
        "depends_on": {"macros": [], "nodes": [model_uid]},
        "meta": {"owner": "data-team"},
        "tags": ["critical"],
    }


def _source_node(idx: int, pkg: str) -> dict[str, Any]:
    """Build a source node."""
    source_name = f"source_{idx // 10}"
    table = f"table_{idx}"
    unique_id = f"source.{pkg}.{source_name}.{table}"
    return {
        **_DEFAULT_SOURCE,
        "name": table,
        "identifier": table,
        "source_name": source_name,
        "unique_id": unique_id,
        "package_name": pkg,
        "fqn": [pkg, source_name, table],
        "original_file_path": "models/staging/crm/_crm__sources.yml",
        "path": "models/staging/crm/_crm__sources.yml",
        "description": "A well documented source table used for benchmarking.",
        "loader": "fivetran",
        "columns": _columns(2),
        "meta": {"owner": "data-team", "contact": {"email": "x@y.z", "slack": "#d"}},
        "tags": ["example_tag"],
        "freshness": {
            "warn_after": {"count": 24, "period": "hour"},
            "error_after": {"count": 48, "period": "hour"},
        },
    }


def _macro_node(idx: int, pkg: str) -> dict[str, Any]:
    """Build a macro node."""
    name = f"macro_{idx}"
    return {
        **_DEFAULT_MACRO,
        "name": name,
        "unique_id": f"macro.{pkg}.{name}",
        "package_name": pkg,
        "original_file_path": f"macros/{name}.sql",
        "path": f"macros/{name}.sql",
        "macro_sql": f"{{% macro {name}(x) %}}\n  select {{{{ x }}}}\n{{% endmacro %}}",
        "description": f"Macro {name} description.",
        "meta": {"owner": "data-team"},
        "arguments": [{"name": "x", "description": "An argument."}],
    }


def _exposure_node(idx: int, pkg: str, model_uid: str) -> dict[str, Any]:
    """Build an exposure node depending on ``model_uid``."""
    name = f"exposure_{idx}"
    return {
        **_DEFAULT_EXPOSURE,
        "name": name,
        "unique_id": f"exposure.{pkg}.{name}",
        "package_name": pkg,
        "original_file_path": "models/marts/finance/_exposures.yml",
        "path": "marts/finance/_exposures.yml",
        "fqn": [pkg, "marts", "finance", name],
        "description": f"Exposure {name} description.",
        "depends_on": {"nodes": [model_uid]},
        "meta": {"maturity": "high", "owner": "data-team"},
        "owner": {"email": "anna@example.com", "name": "Anna Anderson"},
    }


def _semantic_model_node(idx: int, pkg: str, model_uid: str) -> dict[str, Any]:
    """Build a semantic model node."""
    name = f"semantic_model_{idx}"
    return {
        **_DEFAULT_SEMANTIC_MODEL,
        "name": name,
        "unique_id": f"semantic_model.{pkg}.{name}",
        "package_name": pkg,
        "original_file_path": "models/marts/finance/_semantic_models.yml",
        "path": "marts/finance/_semantic_models.yml",
        "fqn": [pkg, "marts", "finance", name],
        "depends_on": {"nodes": [model_uid]},
    }


def _unit_test_node(idx: int, pkg: str, model_uid: str, model_name: str) -> dict:
    """Build a unit-test node covering ``model_uid``."""
    name = f"unit_test_{idx}"
    return {
        **_DEFAULT_UNIT_TEST,
        "name": name,
        "unique_id": f"unit_test.{pkg}.{model_name}.{name}",
        "package_name": pkg,
        "model": model_name,
        "original_file_path": "models/marts/finance/_unit_tests.yml",
        "path": "marts/finance/_unit_tests.yml",
        "fqn": [pkg, "marts", "finance", model_name, name],
        "depends_on": {"nodes": [model_uid]},
        "expect": {"format": "dict", "rows": [{"col_0": 1}]},
        "given": [{"input": f"ref('{model_name}')", "format": "dict", "rows": []}],
    }


def _seed_node(idx: int, pkg: str) -> dict[str, Any]:
    """Build a seed node."""
    name = f"raw_seed_{idx}"
    return {
        **_DEFAULT_SEED,
        "name": name,
        "alias": name,
        "unique_id": f"seed.{pkg}.{name}",
        "package_name": pkg,
        "original_file_path": f"seeds/{name}.csv",
        "path": f"{name}.csv",
        "fqn": [pkg, name],
        "columns": _columns(2),
        "description": f"Seed {name} description.",
        "meta": {"owner": "data-team"},
    }


def _snapshot_node(idx: int, pkg: str) -> dict[str, Any]:
    """Build a snapshot node."""
    name = f"snapshot_{idx}"
    return {
        **_DEFAULT_SNAPSHOT,
        "name": name,
        "alias": name,
        "unique_id": f"snapshot.{pkg}.{name}",
        "package_name": pkg,
        "original_file_path": f"snapshots/{name}.sql",
        "path": f"{name}.sql",
        "fqn": [pkg, name],
        "description": f"Snapshot {name} description.",
        "meta": {"maturity": "high", "owner": "data-team"},
        "tags": ["payment"],
        "config": {
            "materialized": "snapshot",
            "unique_key": "col_0",
            "strategy": "check",
            "check_cols": ["col_0"],
        },
    }


def build_manifest(
    n_models: int = 5000,
    *,
    package_name: str = DEFAULT_PACKAGE_NAME,
    dbt_version: str = DEFAULT_DBT_VERSION,
) -> dict[str, Any]:
    """Build a synthetic dbt manifest dict.

    Every resource shares ``package_name`` and the manifest ``project_name`` is
    set to match, since the parser filters all resources by package name.

    Args:
        n_models: Total number of models (split ~50/25/25 across
            staging/intermediate/marts).
        package_name: Package name shared by every resource.
        dbt_version: dbt version stamped in metadata (must be >= 1.8.0 for
            unit_tests to be parsed).

    Returns:
        A raw manifest dict ready to be JSON-serialised.
    """
    pkg = package_name
    n_staging = n_models // 2
    n_intermediate = n_models // 4
    n_marts = n_models - n_staging - n_intermediate

    nodes: dict[str, dict[str, Any]] = {}
    sources: dict[str, dict[str, Any]] = {}
    macros: dict[str, dict[str, Any]] = {}
    exposures: dict[str, dict[str, Any]] = {}
    semantic_models: dict[str, dict[str, Any]] = {}
    unit_tests: dict[str, dict[str, Any]] = {}

    # --- Sources (referenced by staging models) ---
    n_sources = max(10, n_models // 20)
    source_uids: list[str] = []
    for i in range(n_sources):
        src = _source_node(i, pkg)
        sources[src["unique_id"]] = src
        source_uids.append(src["unique_id"])

    # --- Macros ---
    n_macros = max(50, n_models // 15)
    for i in range(n_macros):
        mac = _macro_node(i, pkg)
        macros[mac["unique_id"]] = mac

    # --- Models, layer by layer, wiring depends_on upstream ---
    staging_uids: list[str] = []
    intermediate_uids: list[str] = []
    marts_uids: list[str] = []
    idx = 0

    for _ in range(n_staging):
        parent_source = source_uids[idx % n_sources]
        node = _model_node(idx, "staging", pkg, [parent_source])
        nodes[node["unique_id"]] = node
        staging_uids.append(node["unique_id"])
        idx += 1

    for j in range(n_intermediate):
        # Depend on 2 staging models (deterministic spread).
        parents = [
            staging_uids[(j * 2) % n_staging],
            staging_uids[(j * 2 + 1) % n_staging],
        ]
        node = _model_node(idx, "intermediate", pkg, parents)
        nodes[node["unique_id"]] = node
        intermediate_uids.append(node["unique_id"])
        idx += 1

    for j in range(n_marts):
        # Depend on 2 intermediate models.
        parents = [
            intermediate_uids[(j * 2) % max(1, n_intermediate)],
            intermediate_uids[(j * 2 + 1) % max(1, n_intermediate)],
        ]
        node = _model_node(idx, "marts", pkg, parents)
        nodes[node["unique_id"]] = node
        marts_uids.append(node["unique_id"])
        idx += 1

    all_model_uids = staging_uids + intermediate_uids + marts_uids

    # --- Tests: unique + not_null on every model ---
    test_idx = 0
    for uid in all_model_uids:
        model_name = uid.split(".")[-1]
        for kind in ("unique", "not_null"):
            test = _test_node(test_idx, uid, model_name, pkg, kind)
            nodes[test["unique_id"]] = test
            test_idx += 1

    # --- Seeds & snapshots ---
    for i in range(max(5, n_models // 250)):
        seed = _seed_node(i, pkg)
        nodes[seed["unique_id"]] = seed
    for i in range(max(5, n_models // 250)):
        snap = _snapshot_node(i, pkg)
        nodes[snap["unique_id"]] = snap

    # --- Exposures / semantic models / unit tests (over marts) ---
    for i in range(max(10, n_marts // 25)):
        model_uid = marts_uids[i % max(1, n_marts)]
        exp = _exposure_node(i, pkg, model_uid)
        exposures[exp["unique_id"]] = exp
    for i in range(max(10, n_marts // 12)):
        model_uid = marts_uids[i % max(1, n_marts)]
        sm = _semantic_model_node(i, pkg, model_uid)
        semantic_models[sm["unique_id"]] = sm
    for i in range(max(10, n_marts // 5)):
        model_uid = marts_uids[i % max(1, n_marts)]
        model_name = model_uid.split(".")[-1]
        ut = _unit_test_node(i, pkg, model_uid, model_name)
        unit_tests[ut["unique_id"]] = ut

    # --- parent_map / child_map from every node's depends_on ---
    parent_map: dict[str, list[str]] = {}
    child_map: dict[str, list[str]] = {uid: [] for uid in nodes}
    child_map.update({uid: [] for uid in sources})
    for uid, node in nodes.items():
        parents = list(node.get("depends_on", {}).get("nodes", []) or [])
        parent_map[uid] = parents
        for parent in parents:
            child_map.setdefault(parent, []).append(uid)
    for uid in sources:
        parent_map.setdefault(uid, [])

    manifest = {
        **_DEFAULT_MANIFEST,
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": dbt_version,
            "project_name": pkg,
            "adapter_type": "duckdb",
        },
        "nodes": nodes,
        "sources": sources,
        "macros": macros,
        "exposures": exposures,
        "semantic_models": semantic_models,
        "unit_tests": unit_tests,
        "parent_map": parent_map,
        "child_map": child_map,
    }
    return manifest


def build_catalog(manifest: dict[str, Any]) -> dict[str, Any]:
    """Build a ``catalog.json`` dict matching the manifest's models/seeds/sources."""
    catalog_nodes: dict[str, dict[str, Any]] = {}
    catalog_sources: dict[str, dict[str, Any]] = {}

    for uid, node in manifest["nodes"].items():
        if node.get("resource_type") not in {"model", "seed", "snapshot"}:
            continue
        columns = {
            col_name: {
                "index": col["index"],
                "name": col_name,
                "type": col.get("data_type", "INTEGER"),
                "comment": None,
            }
            for col_name, col in (node.get("columns") or {}).items()
        }
        catalog_nodes[uid] = {
            **_DEFAULT_CATALOG_NODE,
            "unique_id": uid,
            "columns": columns or _DEFAULT_CATALOG_NODE["columns"],
            "metadata": {
                "name": node["name"],
                "schema": node.get("schema", "main"),
                "type": "VIEW",
            },
            "stats": {},
        }

    for uid, src in manifest["sources"].items():
        columns = {
            col_name: {"index": col["index"], "name": col_name, "type": "INTEGER"}
            for col_name, col in (src.get("columns") or {}).items()
        }
        catalog_sources[uid] = {
            **_DEFAULT_CATALOG_SOURCE,
            "unique_id": uid,
            "columns": columns or _DEFAULT_CATALOG_SOURCE["columns"],
            "metadata": {
                "name": src["name"],
                "schema": src.get("schema", "main"),
                "type": "BASE TABLE",
            },
            "stats": {},
        }

    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json",
            "dbt_version": manifest["metadata"]["dbt_version"],
        },
        "nodes": catalog_nodes,
        "sources": catalog_sources,
        "errors": None,
    }


def build_run_results(manifest: dict[str, Any]) -> dict[str, Any]:
    """Build a ``run_results.json`` dict with one result per executable node."""
    results = []
    for uid, node in manifest["nodes"].items():
        if node.get("resource_type") not in {"model", "seed", "snapshot", "test"}:
            continue
        results.append(
            {
                **_DEFAULT_RUN_RESULT,
                "unique_id": uid,
                "status": "success",
                "execution_time": 0.5,
                "adapter_response": {"rows_affected": 1},
                "timing": [],
            }
        )
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "dbt_version": manifest["metadata"]["dbt_version"],
        },
        "results": results,
        "args": {},
        "elapsed_time": 1.0,
    }


def write_artifacts(
    out_dir: Path,
    n_models: int = 5000,
    *,
    package_name: str = DEFAULT_PACKAGE_NAME,
    with_catalog: bool = True,
    with_run_results: bool = True,
) -> Path:
    """Generate and write manifest (and optionally catalog/run_results) to disk.

    Args:
        out_dir: Directory to write the artifacts into (created if missing).
        n_models: Total number of models to generate.
        package_name: Package name shared by every resource.
        with_catalog: Also write ``catalog.json``.
        with_run_results: Also write ``run_results.json``.

    Returns:
        The path to the written ``manifest.json``.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = build_manifest(n_models, package_name=package_name)
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_bytes(orjson.dumps(manifest))
    if with_catalog:
        (out_dir / "catalog.json").write_bytes(orjson.dumps(build_catalog(manifest)))
    if with_run_results:
        (out_dir / "run_results.json").write_bytes(
            orjson.dumps(build_run_results(manifest))
        )
    return manifest_path


def main(
    out_dir: Annotated[
        Path,
        typer.Option(
            help="Directory to write manifest.json (and catalog/run_results) into."
        ),
    ],
    models: Annotated[
        int | None,
        typer.Option(
            help="Total number of models to generate "
            "(default 5000 or $DBT_BOUNCER_BENCH_MODELS).",
        ),
    ] = None,
    package_name: Annotated[
        str,
        typer.Option(help="Package name shared by every resource."),
    ] = DEFAULT_PACKAGE_NAME,
    catalog: Annotated[
        bool,
        typer.Option(help="Also write catalog.json."),
    ] = True,
    run_results: Annotated[
        bool,
        typer.Option(help="Also write run_results.json."),
    ] = True,
) -> None:
    """Materialise synthetic dbt artifacts to disk."""
    n_models = models if models is not None else _env_model_count(5000)
    manifest_path = write_artifacts(
        out_dir,
        n_models,
        package_name=package_name,
        with_catalog=catalog,
        with_run_results=run_results,
    )
    size_mb = manifest_path.stat().st_size / 1_000_000
    typer.echo(f"Wrote {n_models} models -> {manifest_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    typer.run(main)
