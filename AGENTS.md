# dbt-bouncer

dbt-bouncer enforces conventions for dbt projects by running validation checks against dbt artifacts (manifest, catalog, run results). See [README](README.md) for full project details and [CONTRIBUTING](docs/CONTRIBUTING.md) for contribution guidelines.

## Setup

- **Python:** 3.11+
- **Package manager:** [uv](https://docs.astral.sh/uv/)
- **Dev Container:** supported (VSCode / GitHub Codespaces)

```bash
make install
```

## Common Commands

| Command | Description |
|---|---|
| `make test` | Run all tests (unit + integration) |
| `make test-unit` | Run unit tests only |
| `make test-integration` | Run integration tests only |
| `prek run --all-files` | Run pre-commit hooks (**not** `pre-commit run`) |
| `make build-and-run-dbt-bouncer` | End-to-end validation |
| `make build-artifacts` | Regenerate test fixtures (dbt 1.9, 1.10, 1.11) |
| `make test-perf` | Performance benchmarks (bencher + hyperfine) |

## Architecture

### Directory Layout

- `src/dbt_bouncer/checks/` — checks organised by artifact type:
  - `catalog/` — catalog checks (`columns/` subdirectory for column-level)
  - `manifest/` — manifest checks (`models/` has 11 files by concern: access, code, columns, description, directories, lineage, meta, naming, tags, tests, versioning; `sources/` similarly split)
  - `run_results/` — run results checks
- `src/dbt_bouncer/check_base.py` — `BaseCheck` (Pydantic model), all checks inherit from this
- `src/dbt_bouncer/check_patterns.py` — abstract base classes for common patterns
- `src/dbt_bouncer/runner.py` — orchestrates check execution
- `src/dbt_bouncer/executor.py` — parallel execution with `ThreadPoolExecutor`
- `tests/` — mirrors `src/` structure; fixtures in `tests/fixtures/`

### Check System

All checks inherit from `BaseCheck`. Before writing a check from scratch, check if a pattern ABC in `check_patterns.py` fits:

- `BaseNamePatternCheck` — validates resource name against regex
- `BaseDescriptionPopulatedCheck` — checks description is populated
- `BaseColumnsHaveTypesCheck` — all columns have `data_type`
- `BaseHasUnitTestsCheck` — minimum unit test count
- `BaseHasTagsCheck` — required tags (any/all/one criteria)
- `BaseHasMetaKeysCheck` — required keys in meta config

### Runner Flow

1. `runner.py` iterates checks, determines the resource type from class annotations
2. Calls `set_resource()` to inject the per-iteration resource into the check instance
3. `Executor` batches checks by class and runs them via `ThreadPoolExecutor`
4. Failed checks raise `DbtBouncerFailedCheckError` (from `dbt_bouncer.checks.common`)

**Resource types:** catalog_node, catalog_source, exposure, macro, model, run_result, seed, semantic_model, snapshot, source, test, unit_test

## Writing a New Check

**Naming:** class `Check<ResourceType>Xxx` (e.g. `CheckModelHasDescription`, `CheckExposureBasedOnModel`), name field is the snake_case equivalent.

**Template:**

```python
from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class CheckModelXxx(BaseCheck):
    """One-line description of the check.

    Parameters:
        param_name (type): Description. (omit this section if no extra params)

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_xxx
        ```

    """

    model: Any | None = Field(default=None)
    name: Literal["check_model_xxx"]

    def execute(self) -> None:
        """Execute the check."""
        model = self._require_model()
        if not some_condition:
            raise DbtBouncerFailedCheckError(
                f"`{model.unique_id}` failed: reason."
            )
```

**Steps after writing:**

1. Place in the appropriate submodule under `src/dbt_bouncer/checks/`
2. Add to `dbt-bouncer-example.yml` and validate: `dbt-bouncer --config-file dbt-bouncer-example.yml`
3. Write tests (happy + unhappy paths) in the mirror location under `tests/unit/checks/`
4. Run `make test-unit` and `prek run --all-files`

## Testing

- Test files mirror `src/` structure under `tests/unit/checks/`
- **Happy path + unhappy path** test cases required
- `__init__.py` required in test subdirectories
- Shared fixtures in `conftest.py` files (session-scoped and per-directory)
- Run: `make test-unit`

## Key Constraints

- Use `prek run --all-files`, **not** `pre-commit run`
- Use `[project.optional-dependencies]` for dev deps, **not** `uv add --dev`
- Do not manually edit generated fixture JSON files in `tests/fixtures/`
- Maintain **alphabetical ordering** everywhere (checks, dependencies, lists)
- Do not modify `.pre-commit-config.yaml` to fix lint errors
- Append `# ty: ignore` to disable the `ty` type checker on a specific line
