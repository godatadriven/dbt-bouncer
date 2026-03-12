---
name: new-check
description: Scaffold a new dbt-bouncer check class with tests
---

# Create a New Check

Follow these steps to add a new check to dbt-bouncer.

## 1. Determine Check Location

- **Category:** manifest, catalog, or run_results?
- **Resource type:** model, source, seed, exposure, macro, etc.?
- **File:** place in the appropriate submodule under `src/dbt_bouncer/checks/<category>/`. For model checks, pick the correct concern file in `manifest/models/` (access, code, columns, description, directories, lineage, meta, naming, tags, tests, or versioning).

## 2. Check for Pattern ABCs

Before writing from scratch, check `src/dbt_bouncer/check_patterns.py` for a base class that fits:

| ABC | Use When |
|---|---|
| `BaseNamePatternCheck` | Validating resource name against a regex |
| `BaseDescriptionPopulatedCheck` | Checking description is populated |
| `BaseColumnsHaveTypesCheck` | All columns must have `data_type` |
| `BaseHasUnitTestsCheck` | Minimum unit test count |
| `BaseHasTagsCheck` | Required tags with criteria |
| `BaseHasMetaKeysCheck` | Required keys in meta config |

If one fits, inherit from it instead of `BaseCheck`.

## 3. Write the Check Class

- Class name: `Check<ResourceType>Xxx` (e.g. `CheckModelHasDescription`)
- Name field: snake_case equivalent as a `Literal` (e.g. `Literal["check_model_has_description"]`)
- Docstring must include: description, Parameters (if applicable), Receives, Other Parameters, Example(s) sections
- Use `_require_*()` methods (e.g. `self._require_model()`) to get the resource
- Raise `DbtBouncerFailedCheckError` on failure (from `dbt_bouncer.checks.common`)
- See `AGENTS.md` for the full template

## 4. Register the Check

- Add the check to `dbt-bouncer-example.yml`
- Validate: `dbt-bouncer --config-file dbt-bouncer-example.yml`
- Ensure alphabetical ordering is maintained

## 5. Write Tests

- Create test in the mirror location: `tests/unit/checks/<category>/test_<file>.py`
- Include at least one happy path and one unhappy path test
- Ensure `__init__.py` exists in the test subdirectory
- Use fixtures from the nearest `conftest.py`

## 6. Verify

```bash
make test-unit
prek run --all-files
```
