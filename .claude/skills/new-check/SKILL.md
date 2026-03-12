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

## 2. Choose: Decorator or Class-Based

**Use the `@check` decorator** (recommended) for most checks:

```python
from dbt_bouncer.check_decorator import check, fail

@check("check_model_xxx", iterate_over="model", params={"my_param": str})
def check_model_xxx(model, ctx, *, my_param: str):
    """Check description."""
    if some_condition:
        fail(f"`{model.unique_id}` failed because ...")
```

**Use the class-based approach** when you need a pattern ABC from `check_patterns.py`:

| ABC | Use When |
|---|---|
| `BaseNamePatternCheck` | Validating resource name against a regex |
| `BaseDescriptionPopulatedCheck` | Checking description is populated |
| `BaseColumnsHaveTypesCheck` | All columns must have `data_type` |
| `BaseHasUnitTestsCheck` | Minimum unit test count |
| `BaseHasTagsCheck` | Required tags with criteria |
| `BaseHasMetaKeysCheck` | Required keys in meta config |

## 3. Decorator API Reference

```python
@check(
    name,                    # "check_model_xxx" — used in YAML config
    iterate_over="model",    # resource type, or omit for context-only checks
    params={                 # user-configurable params (appear in YAML)
        "required_param": str,
        "optional_param": (int, 10),  # (type, default) for optional
    },
)
def check_model_xxx(model, ctx, *, required_param: str, optional_param: int):
    """Docstring becomes the check description."""
    # model = the current resource (auto-injected)
    # ctx = CheckContext with ctx.models, ctx.sources, etc.
    fail("message")  # raises DbtBouncerFailedCheckError
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
