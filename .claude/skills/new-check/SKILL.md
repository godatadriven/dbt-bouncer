---
name: new-check
description: Scaffold a new dbt-bouncer check class with tests
---

# Create a New Check

Follow these steps to add a new check to dbt-bouncer.

## 1. Determine Check Location

- **Category:** manifest, catalog, or run_results?
- **Resource type:** model, source, seed, exposure, macro, etc.?
- **File:** place in the appropriate submodule under `src/dbt_bouncer/checks/<category>/`.

## 2. Write the Check

Use the `@check` decorator (bare, no arguments). Everything is inferred from the function signature:

```python
from dbt_bouncer.check_decorator import check, fail

@check
def check_model_xxx(model):
    """Check description."""
    if some_condition:
        fail(f"`{model.unique_id}` failed because ...")
```

## 3. Decorator API Reference

`@check` is a bare decorator — it takes **no arguments**. All metadata is inferred from the function signature:

- **name** — the function name (must match the `name:` value in YAML config).
- **iterate_over** — the first positional parameter (excluding `ctx`). If there are none, the check is global (runs once with context only).
- **params** — keyword-only arguments (after `*`) become user-configurable Pydantic fields.
- **ctx** — optional; only include in the signature if the function actually uses it.
- **Parameter ordering** — must be `(resource, ctx, *, params)`. Resource first, `ctx` second. Putting `ctx` before the resource breaks iterate_over inference. For context-only checks, use `(ctx, *, params)`.

### Simple check (resource only)

```python
@check
def check_model_description_populated(model):
    """Models must have a populated description."""
    if not model.description or len(model.description.strip()) < 4:
        fail(f"`{model.unique_id}` does not have a populated description.")
```

### Check with params

```python
@check
def check_model_names(model, *, model_name_pattern: str):
    """Models must have a name matching the supplied regex."""
    import re
    if not re.match(model_name_pattern, model.name, re.IGNORECASE):
        fail(f"`{model.unique_id}` does not match pattern `{model_name_pattern}`.")
```

### Context-only check (no resource iteration)

```python
@check
def check_model_test_coverage(ctx, *, min_model_test_coverage_pct: float = 100):
    """Set the minimum percentage of models that have at least one test."""
    ...
```

### `fail()` — raises `DbtBouncerFailedCheckError`

```python
fail("message")
```

## 4. Register the Check

- Add the check to `dbt-bouncer-example.yml`
- Validate: `dbt-bouncer --config-file dbt-bouncer-example.yml`
- Ensure alphabetical ordering is maintained

## 5. Write Tests

Use `check_passes` / `check_fails` from `dbt_bouncer.testing`:

```python
from dbt_bouncer.testing import check_fails, check_passes

def test_pass():
    check_passes("check_model_xxx", model={"name": "valid"}, my_param="value")

def test_fail():
    check_fails("check_model_xxx", model={"name": "invalid"}, my_param="value")

# For context-dependent checks:
def test_with_context():
    check_passes("check_model_xxx",
                 model={"name": "m1"},
                 ctx_models=[{"name": "m1"}, {"name": "m2"}])
```

- Resource dicts are auto-merged with sensible defaults (no fixture setup needed)
- `ctx_*` kwargs build the `CheckContext` automatically
- Include at least one happy path and one unhappy path test
- Ensure `__init__.py` exists in the test subdirectory

## 6. Verify

```bash
make test-unit
prek run --all-files
```
