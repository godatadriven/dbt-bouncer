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
make build-artifacts
```

This generates fixtures for dbt 1.10 and 1.11 in `tests/fixtures/dbt_1X/target/` (manifest.json, catalog.json, run_results.json). Note: dbt 1.9 fixtures are frozen and not regenerated.

**Note:** The Makefile uses specific dbt-duckdb version pins. Do not modify the version pins in the Makefile without understanding the compatibility matrix.

### 2. Verify the fixtures

Check that the generated files exist and are non-empty:

```bash
ls -la tests/fixtures/dbt_110/target/
ls -la tests/fixtures/dbt_111/target/
```

### 3. Run tests

```bash
make test
```

All tests should pass with the new fixtures. If tests fail, check whether the fixture schema changed in a way that requires test updates.
