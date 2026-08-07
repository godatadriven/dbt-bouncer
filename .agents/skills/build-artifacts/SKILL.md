---
name: build-artifacts
description: Regenerate dbt test fixtures after dbt_project changes
---

# Build Artifacts

Regenerate test fixture files after making changes to the dbt project in `dbt_project/`.

## When to Use

- After modifying models, seeds, or tests in `dbt_project/`
- After upgrading dbt version dependencies
- When tests fail because fixture data is stale

## Steps

### 1. Run the build

```bash
mise run build-artifacts
```

This generates fixtures for dbt 1.12 and 2.0 in `tests/fixtures/dbt_112/target/` and `tests/fixtures/dbt_20/target/` (manifest.json, catalog.json, run_results.json). The dbt 1.7–1.11 fixtures are frozen: they stay committed for backward-compat parsing coverage but are no longer rebuilt.

**Note:** `mise.toml` uses specific dbt-duckdb version pins. Do not modify the version pins in `mise.toml` without understanding the compatibility matrix.

### 2. Verify the fixtures

Check that the generated files exist and are non-empty. `catalog.json` is built by introspecting the DuckDB database, so a fixture built without the preceding `dbt build` step silently ends up with an empty `nodes` object and every catalog check then passes vacuously — check the node count, not just the file size:

```bash
for v in dbt_112 dbt_20; do
  python -c "import json;c=json.load(open('tests/fixtures/$v/target/catalog.json'));print('$v', len(c['nodes']), 'catalog nodes')"
done
```

### 3. Run tests

```bash
mise run test
```

All tests should pass with the new fixtures. If tests fail, check whether the fixture schema changed in a way that requires test updates.
