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

Use the `@check` decorator, passing the rule code. Everything else is inferred from the function signature:

```python
from dbt_bouncer.check_framework.decorator import check, fail

@check(code="XX000")
def check_model_xxx(model):
    """Check description."""
    if some_condition:
        fail(f"`{model.unique_id}` failed because ...")
```

## 3. Decorator API Reference

`code` is the only argument `@check` takes. All other metadata is inferred from the function signature:

- **code** — the check's unique rule code, e.g. `MO048`. See "Assign a rule code" below.
- **name** — the function name (must match the `name:` value in YAML config).
- **iterate_over** — the first positional parameter (excluding `ctx`). If there are none, the check is global (runs once with context only).
- **params** — keyword-only arguments (after `*`) become user-configurable Pydantic fields.
- **ctx** — optional; only include in the signature if the function actually uses it.
- **Parameter ordering** — must be `(resource, ctx, *, params)`. Resource first, `ctx` second. Putting `ctx` before the resource breaks iterate_over inference. For context-only checks, use `(ctx, *, params)`.

### Simple check (resource only)

```python
@check(code="MO021")
def check_model_description_populated(model):
    """Models must have a populated description."""
    if not model.description or len(model.description.strip()) < 4:
        fail(f"`{model.unique_id}` does not have a populated description.")
```

### Check with params

```python
@check(code="MO038")
def check_model_names(model, *, model_name_pattern: str):
    """Models must have a name matching the supplied regex."""
    import re
    if not re.match(model_name_pattern, model.name, re.IGNORECASE):
        fail(f"`{model.unique_id}` does not match pattern `{model_name_pattern}`.")
```

### Context-only check (no resource iteration)

```python
@check(code="MO044")
def check_model_test_coverage(ctx, *, min_model_test_coverage_pct: float = 100):
    """Set the minimum percentage of models that have at least one test."""
    ...
```

### `fail()` — raises `DbtBouncerFailedCheckError`

```python
fail("message")
```

### Assign a rule code

Every check needs a unique rule code: a 2-letter resource prefix plus a 3-digit number, e.g. `MO048`. Two steps:

1. Pass it to the decorator: `@check(code="MO048")`.
2. Add the matching member to the resource's `*RuleCode` enum in `src/dbt_bouncer/enums.py`, keeping alphabetical order:

    ```python
    class ModelRuleCode(StrEnum):
        CHECK_MODEL_XXX = "MO048"
    ```

Use the next free number for the prefix — read the enum to find it. Never reuse or renumber a published code; users reference codes in their config.

Prefixes: `CA` catalog, `EX` exposure, `LI` lineage, `MA` macro, `ME` metadata, `MO` model, `RR` run results, `SE` seed, `SM` semantic model, `SN` snapshot, `SO` source, `TE` test, `UT` unit test.

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
mise run generate-schema
mise run generate-rule-codes-doc
mise run test-unit
prek run --all-files
```

The `rule-codes-doc-check` hook fails if a check has no code, if a declared code is unused, or if `docs/checks/rule_codes.md` has drifted.
