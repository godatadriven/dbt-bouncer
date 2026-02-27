# ResourceWrapper Wiring Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Wire `ResourceWrapper` Protocol into `runner.py` and `utils.py` so `ty` enforces the contract, fix the inaccurate step-7 docstring, and rewrite the test to use direct imports and `issubclass`.

**Architecture:** All changes are on branch `feat/resource-wrapper-protocol` (PR #690). Four targeted edits across three files plus the test. No new files. No new dependencies.

**Tech Stack:** Python typing (`Protocol`, `runtime_checkable`), Pydantic v2, pytest, `ty` (type checker run via pre-commit `prek`).

---

## Task 0: Check out the PR branch

**Step 1: Switch to the PR branch**

```bash
git fetch origin feat/resource-wrapper-protocol
git checkout feat/resource-wrapper-protocol
```

Expected: working directory on `feat/resource-wrapper-protocol`.

---

### Task 1: Fix the inaccurate step-7 docstring

**Files:**

- Modify: `src/dbt_bouncer/resource_adapter.py:15`

**Step 1: Confirm the current text**

```bash
sed -n '13,17p' src/dbt_bouncer/resource_adapter.py
```

Expected output:

```text
5. Add the resource field to ``BaseCheck`` in ``check_base.py``.
6. Add a ``_require_<resource>`` guard method to ``BaseCheck``.
7. Create a ``checks/<resource>/`` directory with the check modules.
```

**Step 2: Replace line 15**

Open `src/dbt_bouncer/resource_adapter.py` and change line 15 from:

```text
7. Create a ``checks/<resource>/`` directory with the check modules.
```

to:

```text
7. Add check modules to the appropriate existing directory: ``checks/catalog/``,
   ``checks/manifest/``, or ``checks/run_results/``.
```

**Step 3: Verify**

```bash
sed -n '13,18p' src/dbt_bouncer/resource_adapter.py
```

**Step 4: Commit**

```bash
git add src/dbt_bouncer/resource_adapter.py
git commit -m "docs: fix step 7 in ResourceWrapper guide — checks/ is grouped by artifact type"
```

---

### Task 2: Wire `ResourceWrapper` into `utils.py`

**Files:**

- Modify: `src/dbt_bouncer/utils.py:20,95,551`

`utils.py` contains two copies of every function (file was accidentally doubled). Both `resource_in_path` definitions need the same change.

**Step 1: Add the import — first TYPE_CHECKING block (around line 20)**

`utils.py` has `if TYPE_CHECKING:` at around line 20. Add `ResourceWrapper` there.

Current block (first occurrence, around line 20):

```python
if TYPE_CHECKING:
    from dbt_bouncer.check_base import BaseCheck
```

Change to:

```python
if TYPE_CHECKING:
    from dbt_bouncer.check_base import BaseCheck
    from dbt_bouncer.resource_adapter import ResourceWrapper
```

**Step 2: Add the import — second TYPE_CHECKING block (around line 476)**

The second copy of the same block (around line 476, inside the duplicate section) needs the same change:

```python
if TYPE_CHECKING:
    from dbt_bouncer.check_base import BaseCheck
    from dbt_bouncer.resource_adapter import ResourceWrapper
```

**Step 3: Annotate the first `resource_in_path` (line ~95)**

Change:

```python
def resource_in_path(check, resource) -> bool:
```

to:

```python
def resource_in_path(check, resource: "ResourceWrapper") -> bool:
```

**Step 4: Annotate the second `resource_in_path` (line ~551)**

Same change to the duplicate definition:

```python
def resource_in_path(check, resource: "ResourceWrapper") -> bool:
```

**Step 5: Verify no import errors**

```bash
python -c "from dbt_bouncer.utils import resource_in_path; print('OK')"
```

Expected: `OK`

**Step 6: Commit**

```bash
git add src/dbt_bouncer/utils.py
git commit -m "feat: annotate resource_in_path with ResourceWrapper type"
```

---

### Task 3: Wire `ResourceWrapper` into `runner.py`

**Files:**

- Modify: `src/dbt_bouncer/runner.py`

**Step 1: Add import to the TYPE_CHECKING block**

Find the `if TYPE_CHECKING:` block (around line 27):

```python
if TYPE_CHECKING:
    from dbt_bouncer.context import BouncerContext
```

Change to:

```python
if TYPE_CHECKING:
    from dbt_bouncer.context import BouncerContext
    from dbt_bouncer.resource_adapter import ResourceWrapper
```

**Step 2: Annotate `resource_map`**

Find the `resource_map` assignment (around line 107):

```python
    resource_map: dict[str, list[Any]] = {
```

Change to:

```python
    resource_map: dict[str, list["ResourceWrapper"]] = {
```

**Step 3: Verify**

```bash
python -c "from dbt_bouncer.runner import runner; print('OK')"
```

Expected: `OK`

**Step 4: Run the full test suite to catch any regressions**

```bash
uv run pytest tests/unit/ -x -q 2>&1 | tail -5
```

Expected: all tests passing.

**Step 5: Commit**

```bash
git add src/dbt_bouncer/runner.py
git commit -m "feat: type resource_map as dict[str, list[ResourceWrapper]] in runner.py"
```

---

### Task 4: Rewrite the protocol conformance test

**Files:**

- Modify: `tests/unit/test_resource_adapter.py`

The current test imports classes via `importlib` using string paths and walks `__annotations__` manually. Replace with direct imports and `issubclass`.

**Step 1: Rewrite the test file**

Replace the entire file contents with:

```python
"""Tests for the ResourceWrapper protocol."""

import pytest

from dbt_bouncer.artifact_parsers.parsers_catalog import DbtBouncerCatalogNode
from dbt_bouncer.artifact_parsers.parsers_manifest import (
    DbtBouncerExposure,
    DbtBouncerModel,
    DbtBouncerSeed,
    DbtBouncerSemanticModel,
    DbtBouncerSnapshot,
    DbtBouncerSource,
    DbtBouncerTest,
)
from dbt_bouncer.artifact_parsers.parsers_run_results import DbtBouncerRunResult
from dbt_bouncer.resource_adapter import ResourceWrapper


@pytest.mark.parametrize(
    "cls",
    [
        DbtBouncerCatalogNode,
        DbtBouncerExposure,
        DbtBouncerModel,
        DbtBouncerRunResult,
        DbtBouncerSeed,
        DbtBouncerSemanticModel,
        DbtBouncerSnapshot,
        DbtBouncerSource,
        DbtBouncerTest,
    ],
)
def test_wrapper_classes_satisfy_resource_wrapper_protocol(cls: type) -> None:
    """All dbt resource wrapper classes must satisfy the ResourceWrapper protocol."""
    assert issubclass(cls, ResourceWrapper), (
        f"{cls.__name__} does not satisfy ResourceWrapper: "
        "missing 'unique_id' or 'original_file_path'"
    )
```

Note: the parametrize list is alphabetical by class name (`DbtBouncerCatalogNode`, `DbtBouncerExposure`, …, `DbtBouncerTest`). `DbtBouncerRunResult` is sorted between `DbtBouncerModel` and `DbtBouncerSeed`.

**Step 2: Run the tests to confirm all 9 pass**

```bash
uv run pytest tests/unit/test_resource_adapter.py -v 2>&1 | tail -15
```

Expected: 9 PASSED.

**Step 3: Run the full unit suite**

```bash
uv run pytest tests/unit/ -x -q 2>&1 | tail -5
```

Expected: all tests passing.

**Step 4: Run pre-commit to confirm `ty` and ruff are happy**

```bash
prek run --all-files 2>&1 | tail -20
```

Expected: all hooks pass.

**Step 5: Commit**

```bash
git add tests/unit/test_resource_adapter.py
git commit -m "test: rewrite ResourceWrapper test with direct imports and issubclass"
```

---

### Task 5: Push and update the PR

**Step 1: Push the branch**

```bash
git push origin feat/resource-wrapper-protocol
```

**Step 2: Verify CI is triggered**

```bash
gh run list --branch feat/resource-wrapper-protocol --limit 1
```
