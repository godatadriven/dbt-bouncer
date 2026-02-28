# ResourceWrapper Protocol Wiring Design

**Date:** 2026-02-27
**Branch:** feat/resource-wrapper-protocol (PR #690)

## Problem

PR #690 adds a `ResourceWrapper` Protocol but never imports or uses it in production code. Without a type annotation referencing it, `ty` enforces nothing and `@runtime_checkable` is wasted.

## Scope

Four targeted changes to `feat/resource-wrapper-protocol`:

1. **`src/dbt_bouncer/resource_adapter.py`** — Fix step 7 of the "Adding a new resource type" guide: change `checks/<resource>/` to reflect the actual artifact-scoped structure (`checks/catalog/`, `checks/manifest/`, `checks/run_results/`).

2. **`src/dbt_bouncer/utils.py`** — Add `ResourceWrapper` type annotation to the `resource` parameter of `resource_in_path`. This function already accesses `resource.original_file_path`, making it the natural enforcement point.

3. **`src/dbt_bouncer/runner.py`** — Type `resource_map` as `dict[str, list[ResourceWrapper]]`. All values (including raw macro/unit_test objects) satisfy the protocol via structural typing (verified: `Macro_v11` has both `unique_id: str` and `original_file_path: str`).

4. **`tests/unit/test_resource_adapter.py`** — Replace the manual `__annotations__` MRO walk with a Protocol conformance check using `issubclass`. Use direct class imports instead of `"module:Class"` strings. Move `import importlib` to top-level (or remove it entirely once strings are replaced).

## Out of scope

- Exporting `ResourceWrapper` from `__init__.py` (can be done separately if needed for public API).
- Adding `ResourceWrapper` to `check_base.py` fields (those are already specifically typed per-resource).
