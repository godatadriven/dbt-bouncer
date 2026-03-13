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
- `src/dbt_bouncer/check_base.py` — `BaseCheck` (Pydantic model), used by class-based checks
- `src/dbt_bouncer/check_decorator.py` — `@check` decorator and `fail()` helper (preferred API)
- `src/dbt_bouncer/runner.py` — orchestrates check execution
- `src/dbt_bouncer/executor.py` — parallel execution with `ThreadPoolExecutor`
- `tests/` — mirrors `src/` structure; fixtures in `tests/fixtures/`

### Check System

Checks are written using the `@check` decorator (preferred) or as class-based `BaseCheck` subclasses (for plugin authors). The decorator API infers the check name and resource type from the function signature.

### Runner Flow

1. `runner.py` iterates checks, determines the resource type from the function's first positional parameter (decorator API) or class annotations (class-based API)
2. Injects the per-iteration resource into the check
3. `Executor` batches checks by class and runs them via `ThreadPoolExecutor`
4. Failed checks call `fail()` (decorator API) or raise `DbtBouncerFailedCheckError` (class-based API)

**Resource types:** catalog_node, catalog_source, exposure, macro, model, run_result, seed, semantic_model, snapshot, source, test, unit_test

## Writing a New Check

**Naming:** `check_<resource_type>_xxx` (e.g. `check_model_has_description`, `check_exposure_based_on_model`). The check name and resource type are inferred from the function name.

**Template (decorator API — preferred):**

```python
from dbt_bouncer.check_decorator import check, fail


@check
def check_model_xxx(model, *, some_param: str):
    """One-line description of the check.

    Parameters:
        some_param (str): Description. (omit this section if no extra params)

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
              some_param: value
        ```

    """
    if not some_condition:
        fail(f"`{model.unique_id}` failed: reason.")
```

**Key rules:**

- `@check` with no arguments — name and `iterate_over` are inferred from the function name
- First positional parameter (excluding `ctx`) = the resource being checked (e.g. `model`, `source`, `exposure`)
- Keyword-only arguments (after `*`) = user-configurable parameters in YAML
- Add `ctx` as a parameter only when you need access to other resources (e.g. models list, manifest)
- **Parameter ordering must be `(resource, ctx, *, params)`** — resource first, `ctx` second. Putting `ctx` before the resource would cause `ctx` to be treated as the iterate_over target (since the decorator picks the first non-reserved positional param). For context-only checks (no resource), use `(ctx, *, params)`.
- Call `fail()` to signal a check failure

**Steps after writing:**

1. Place in the appropriate submodule under `src/dbt_bouncer/checks/`
2. Add to `dbt-bouncer-example.yml` and validate: `dbt-bouncer --config-file dbt-bouncer-example.yml`
3. Write tests (happy + unhappy paths) in the mirror location under `tests/unit/checks/`
4. Run `make test-unit` and `prek run --all-files`

### Alternative: Class-based checks

For plugin authors or cases requiring custom Pydantic validation, checks can still be written as `BaseCheck` subclasses:

```python
from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class CheckModelXxx(BaseCheck):
    """Docstring with Parameters, Receives, Other Parameters, Example(s) sections."""

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
