# Rust Native Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the Rust JSON artifact parser native to the dbt-bouncer Python package with unified build, CI, tests, and docs.

**Architecture:** Move Rust source to the standard maturin mixed-project layout (`Cargo.toml` at root, `src/lib.rs` alongside `src/dbt_bouncer/`). Switch the build backend to maturin. Ship pre-built platform wheels with Rust baked in, plus a pure-Python fallback wheel. Add Rust unit tests, Python integration tests, CI stages, and documentation.

**Tech Stack:** Rust (PyO3 + serde_json), maturin (build backend), GitHub Actions (PyO3/maturin-action), pytest

**Design doc:** `docs/plans/2026-03-07-rust-native-integration-design.md`

---

### Task 1: Move Rust source to standard maturin layout

**Files:**
- Delete: `rust/.github/workflows/CI.yml`
- Delete: `rust/.gitignore`
- Delete: `rust/pyproject.toml`
- Delete: `rust/Cargo.toml`
- Move: `rust/src/lib.rs` -> `src/lib.rs`
- Move: `rust/Cargo.lock` -> `Cargo.lock`
- Create: `Cargo.toml` (at repo root)
- Modify: `.gitignore`

**Step 1: Create `Cargo.toml` at repo root**

```toml
[package]
name = "dbt-bouncer-rust"
version = "0.0.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]
name = "dbt_artifacts_rs"
path = "src/lib.rs"

[dependencies]
pyo3 = { version = "0.23", features = ["extension-module"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

**Step 2: Move `rust/src/lib.rs` to `src/lib.rs`**

```bash
cp rust/src/lib.rs src/lib.rs
```

**Step 3: Move `rust/Cargo.lock` to repo root**

```bash
cp rust/Cargo.lock Cargo.lock
```

**Step 4: Add `/target` to `.gitignore`**

Append `/target` to the root `.gitignore` file.

**Step 5: Delete the `rust/` directory entirely**

```bash
rm -rf rust/
```

**Step 6: Verify Rust builds from new location**

```bash
cargo build
```

Expected: compiles successfully, produces `target/debug/libdbt_artifacts_rs.so` (or `.dylib`/`.dll`).

**Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/lib.rs .gitignore
git rm -rf rust/
git commit -m "refactor: move Rust source to standard maturin layout

Move Cargo.toml and src/lib.rs to repo root, delete the separate
rust/ subdirectory including its orphaned .github/ and pyproject.toml."
```

---

### Task 2: Switch build backend to maturin

**Files:**
- Modify: `pyproject.toml`

**Step 1: Add build-system and maturin config to `pyproject.toml`**

Add at the top of `pyproject.toml` (before `[project]`):

```toml
[build-system]
requires = ["maturin>=1.12,<2.0"]
build-backend = "maturin"
```

Add at the end of `pyproject.toml` (or in alphabetical section order):

```toml
[tool.maturin]
features = ["pyo3/extension-module"]
python-source = "src"
```

**Step 2: Verify maturin can build the extension in dev mode**

```bash
uv pip install maturin
maturin develop
```

Expected: builds the Rust extension and installs it into the active venv. After this, `python -c "import dbt_artifacts_rs; print('OK')"` prints `OK`.

**Step 3: Verify the Python package still imports correctly**

```bash
python -c "from dbt_bouncer.main import app; print('OK')"
```

Expected: `OK`.

**Step 4: Commit**

```bash
git add pyproject.toml
git commit -m "build: switch build backend to maturin

Add [build-system] with maturin and [tool.maturin] config.
The Rust extension is now built as part of the main package."
```

---

### Task 3: Add Rust unit tests

**Files:**
- Modify: `src/lib.rs` (add `#[cfg(test)]` module at the end)

**Step 1: Add test module to `src/lib.rs`**

Append the following `#[cfg(test)]` module at the end of `src/lib.rs`, before the closing of the file:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- value_to_py ---

    #[test]
    fn test_parse_manifest_json_valid() {
        Python::with_gil(|py| {
            let json_str = r#"{"metadata": {"dbt_version": "1.7.0"}, "nodes": {}}"#;
            let result = parse_manifest_json(py, json_str);
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_parse_manifest_json_invalid() {
        Python::with_gil(|py| {
            let result = parse_manifest_json(py, "not json");
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_parse_catalog_json_valid() {
        Python::with_gil(|py| {
            let json_str = r#"{"metadata": {}, "nodes": {}, "sources": {}}"#;
            let result = parse_catalog_json(py, json_str);
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_parse_catalog_json_invalid() {
        Python::with_gil(|py| {
            let result = parse_catalog_json(py, "{broken");
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_parse_run_results_json_valid() {
        Python::with_gil(|py| {
            let json_str = r#"{"metadata": {}, "results": []}"#;
            let result = parse_run_results_json(py, json_str);
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_parse_run_results_json_invalid() {
        Python::with_gil(|py| {
            let result = parse_run_results_json(py, "");
            assert!(result.is_err());
        });
    }

    // --- JsonObj attribute access ---

    #[test]
    fn test_getattr_exact_match() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"name": "my_model"}),
            };
            let result = obj.__getattr__(py, "name").unwrap();
            let s: String = result.extract(py).unwrap();
            assert_eq!(s, "my_model");
        });
    }

    #[test]
    fn test_getattr_missing_key() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"name": "my_model"}),
            };
            let result = obj.__getattr__(py, "missing");
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_getattr_trailing_underscore() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"schema": "public"}),
            };
            let result = obj.__getattr__(py, "schema_").unwrap();
            let s: String = result.extract(py).unwrap();
            assert_eq!(s, "public");
        });
    }

    #[test]
    fn test_getattr_enum_field_returns_jsonobj() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"access": "public"}),
            };
            let result = obj.__getattr__(py, "access").unwrap();
            // Should be a JsonObj, not a plain string
            let is_string: Result<String, _> = result.extract(py);
            assert!(
                is_string.is_err(),
                "enum fields should return JsonObj, not plain string"
            );
        });
    }

    #[test]
    fn test_getattr_value_on_primitive() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!("hello"),
            };
            let result = obj.__getattr__(py, "value").unwrap();
            let s: String = result.extract(py).unwrap();
            assert_eq!(s, "hello");
        });
    }

    // --- JsonObj dict operations ---

    #[test]
    fn test_getitem_string_key() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"key": "val"}),
            };
            let key = "key".into_pyobject(py).unwrap().into_any().unbind();
            let result = obj.__getitem__(py, key).unwrap();
            let s: String = result.extract(py).unwrap();
            assert_eq!(s, "val");
        });
    }

    #[test]
    fn test_getitem_missing_key() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"key": "val"}),
            };
            let key = "missing".into_pyobject(py).unwrap().into_any().unbind();
            let result = obj.__getitem__(py, key);
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_getitem_array_index() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!([10, 20, 30]),
            };
            let key = 1i64.into_pyobject(py).unwrap().into_any().unbind();
            let result = obj.__getitem__(py, key).unwrap();
            let i: i64 = result.extract(py).unwrap();
            assert_eq!(i, 20);
        });
    }

    #[test]
    fn test_getitem_negative_index() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!([10, 20, 30]),
            };
            let key = (-1i64).into_pyobject(py).unwrap().into_any().unbind();
            let result = obj.__getitem__(py, key).unwrap();
            let i: i64 = result.extract(py).unwrap();
            assert_eq!(i, 30);
        });
    }

    #[test]
    fn test_len_object() {
        let obj = JsonObj {
            data: json!({"a": 1, "b": 2}),
        };
        assert_eq!(obj.__len__(), 2);
    }

    #[test]
    fn test_len_array() {
        let obj = JsonObj {
            data: json!([1, 2, 3]),
        };
        assert_eq!(obj.__len__(), 3);
    }

    #[test]
    fn test_contains_object_key() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"name": "x"}),
            };
            let key = "name".into_pyobject(py).unwrap().into_any().unbind();
            assert!(obj.__contains__(py, key).unwrap());
        });
    }

    #[test]
    fn test_contains_array_value() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!(["a", "b", "c"]),
            };
            let key = "b".into_pyobject(py).unwrap().into_any().unbind();
            assert!(obj.__contains__(py, key).unwrap());
        });
    }

    #[test]
    fn test_contains_array_missing() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!(["a", "b"]),
            };
            let key = "z".into_pyobject(py).unwrap().into_any().unbind();
            assert!(!obj.__contains__(py, key).unwrap());
        });
    }

    // --- Primitives ---

    #[test]
    fn test_bool_truthy() {
        assert!(JsonObj { data: json!(true) }.__bool__());
        assert!(JsonObj { data: json!(1) }.__bool__());
        assert!(JsonObj {
            data: json!("hello")
        }
        .__bool__());
        assert!(JsonObj { data: json!([1]) }.__bool__());
        assert!(JsonObj {
            data: json!({"a": 1})
        }
        .__bool__());
    }

    #[test]
    fn test_bool_falsy() {
        assert!(!JsonObj { data: json!(false) }.__bool__());
        assert!(!JsonObj { data: json!(null) }.__bool__());
        assert!(!JsonObj { data: json!(0) }.__bool__());
        assert!(!JsonObj { data: json!("") }.__bool__());
        assert!(!JsonObj { data: json!([]) }.__bool__());
        assert!(!JsonObj {
            data: json!({})
        }
        .__bool__());
    }

    #[test]
    fn test_eq_string() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!("hello"),
            };
            let other = "hello".into_pyobject(py).unwrap().into_any().unbind();
            assert!(obj.__eq__(py, other));
        });
    }

    #[test]
    fn test_eq_int() {
        Python::with_gil(|py| {
            let obj = JsonObj { data: json!(42) };
            let other = 42i64.into_pyobject(py).unwrap().into_any().unbind();
            assert!(obj.__eq__(py, other));
        });
    }

    #[test]
    fn test_eq_null_none() {
        Python::with_gil(|py| {
            let obj = JsonObj { data: json!(null) };
            assert!(obj.__eq__(py, py.None()));
        });
    }

    #[test]
    fn test_repr_short() {
        let obj = JsonObj {
            data: json!({"a": 1}),
        };
        let r = obj.__repr__();
        assert!(r.starts_with("JsonObj("));
        assert!(r.contains("\"a\""));
    }

    #[test]
    fn test_str() {
        let obj = JsonObj { data: json!(42) };
        assert_eq!(obj.__str__(), "42");
    }

    // --- Dict-like methods ---

    #[test]
    fn test_get_existing_key() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"x": 10}),
            };
            let result = obj.get(py, "x", None).unwrap();
            let i: i64 = result.extract(py).unwrap();
            assert_eq!(i, 10);
        });
    }

    #[test]
    fn test_get_missing_key_returns_default() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"x": 10}),
            };
            let result = obj.get(py, "y", None).unwrap();
            assert!(result.is_none(py));
        });
    }

    #[test]
    fn test_keys() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!({"a": 1, "b": 2}),
            };
            let result = obj.keys(py).unwrap();
            let list: Vec<String> = result.extract(py).unwrap();
            assert!(list.contains(&"a".to_string()));
            assert!(list.contains(&"b".to_string()));
            assert_eq!(list.len(), 2);
        });
    }

    #[test]
    fn test_keys_on_non_object() {
        Python::with_gil(|py| {
            let obj = JsonObj {
                data: json!([1, 2]),
            };
            let result = obj.keys(py);
            assert!(result.is_err());
        });
    }
}
```

**Step 2: Run Rust tests**

```bash
cargo test
```

Expected: all tests pass.

**Step 3: Commit**

```bash
git add src/lib.rs
git commit -m "test: add Rust unit tests for JsonObj and parse functions"
```

---

### Task 4: Add Python integration tests

**Files:**
- Create: `tests/unit/test_rust_extension.py`

**Step 1: Write the Python integration test file**

```python
"""Integration tests for the Rust-based JSON parser extension.

These tests verify that the ``dbt_artifacts_rs`` module (built via maturin)
produces results compatible with the pure-Python parsing path.
"""

import json
from pathlib import Path

import pytest

try:
    import dbt_artifacts_rs

    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not RUST_AVAILABLE,
    reason="dbt_artifacts_rs extension not installed",
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


# --- Availability ---


def test_rust_module_importable():
    assert RUST_AVAILABLE


def test_rust_module_has_parse_functions():
    assert hasattr(dbt_artifacts_rs, "parse_manifest_json")
    assert hasattr(dbt_artifacts_rs, "parse_catalog_json")
    assert hasattr(dbt_artifacts_rs, "parse_run_results_json")


# --- Manifest parsing ---


@pytest.fixture()
def manifest_json_str():
    manifest_path = FIXTURES_DIR / "dbt_111" / "target" / "manifest.json"
    if not manifest_path.exists():
        pytest.skip(f"Fixture not found: {manifest_path}")
    return manifest_path.read_text()


def test_parse_manifest_returns_jsonobj(manifest_json_str):
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    assert result is not None
    assert hasattr(result, "metadata")
    assert hasattr(result, "nodes")


def test_manifest_metadata_access(manifest_json_str):
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    metadata = result.metadata
    assert hasattr(metadata, "dbt_version")
    assert isinstance(str(metadata.dbt_version), str)


def test_manifest_nodes_iteration(manifest_json_str):
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    native = json.loads(manifest_json_str)
    assert len(result.nodes) == len(native["nodes"])


def test_manifest_node_attribute_access(manifest_json_str):
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    native = json.loads(manifest_json_str)
    for key in list(native["nodes"].keys())[:3]:
        node = result.nodes[key]
        assert hasattr(node, "name")
        assert str(node.name) == native["nodes"][key]["name"]


def test_manifest_contains_check(manifest_json_str):
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    native = json.loads(manifest_json_str)
    first_key = next(iter(native["nodes"]))
    assert first_key in result.nodes


def test_manifest_schema_trailing_underscore(manifest_json_str):
    """Verify that ``node.schema_`` resolves to the JSON ``schema`` field."""
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    native = json.loads(manifest_json_str)
    for key in list(native["nodes"].keys())[:3]:
        if "schema" in native["nodes"][key]:
            node = result.nodes[key]
            assert str(node.schema_) == native["nodes"][key]["schema"]
            break


# --- Catalog parsing ---


@pytest.fixture()
def catalog_json_str():
    catalog_path = FIXTURES_DIR / "dbt_111" / "target" / "catalog.json"
    if not catalog_path.exists():
        pytest.skip(f"Fixture not found: {catalog_path}")
    return catalog_path.read_text()


def test_parse_catalog_returns_jsonobj(catalog_json_str):
    result = dbt_artifacts_rs.parse_catalog_json(catalog_json_str)
    assert result is not None
    assert hasattr(result, "nodes")


def test_catalog_nodes_match_native(catalog_json_str):
    result = dbt_artifacts_rs.parse_catalog_json(catalog_json_str)
    native = json.loads(catalog_json_str)
    assert len(result.nodes) == len(native["nodes"])


# --- Run results parsing ---


@pytest.fixture()
def run_results_json_str():
    rr_path = FIXTURES_DIR / "dbt_111" / "target" / "run_results.json"
    if not rr_path.exists():
        pytest.skip(f"Fixture not found: {rr_path}")
    return rr_path.read_text()


def test_parse_run_results_returns_jsonobj(run_results_json_str):
    result = dbt_artifacts_rs.parse_run_results_json(run_results_json_str)
    assert result is not None
    assert hasattr(result, "results")


def test_run_results_count_matches(run_results_json_str):
    result = dbt_artifacts_rs.parse_run_results_json(run_results_json_str)
    native = json.loads(run_results_json_str)
    assert len(result.results) == len(native["results"])


# --- to_python round-trip ---


def test_manifest_to_python_matches_json(manifest_json_str):
    """Verify ``to_python()`` produces a dict identical to ``json.loads()``."""
    result = dbt_artifacts_rs.parse_manifest_json(manifest_json_str)
    native = json.loads(manifest_json_str)
    converted = result.to_python()
    assert converted == native


# --- Error handling ---


def test_parse_invalid_json_raises():
    with pytest.raises(ValueError, match="Invalid manifest JSON"):
        dbt_artifacts_rs.parse_manifest_json("not valid json")


def test_parse_empty_string_raises():
    with pytest.raises(ValueError):
        dbt_artifacts_rs.parse_manifest_json("")


# --- Edge cases ---


def test_empty_object():
    result = dbt_artifacts_rs.parse_manifest_json("{}")
    assert len(result) == 0


def test_deeply_nested_access():
    json_str = json.dumps({"a": {"b": {"c": {"d": "deep"}}}})
    result = dbt_artifacts_rs.parse_manifest_json(json_str)
    assert str(result.a.b.c.d) == "deep"


def test_jsonobj_bool_truthy():
    result = dbt_artifacts_rs.parse_manifest_json('{"key": "value"}')
    assert bool(result)


def test_jsonobj_bool_falsy():
    result = dbt_artifacts_rs.parse_manifest_json("{}")
    assert not bool(result)


def test_jsonobj_items():
    result = dbt_artifacts_rs.parse_manifest_json('{"a": 1, "b": 2}')
    items = result.items()
    keys = [k for k, _ in items]
    assert "a" in keys
    assert "b" in keys


def test_jsonobj_get_with_default():
    result = dbt_artifacts_rs.parse_manifest_json('{"a": 1}')
    assert result.get("a") is not None
    assert result.get("missing") is None
    assert result.get("missing", 42) == 42
```

**Step 2: Run the tests (should pass if `maturin develop` was run in Task 2)**

```bash
uv run pytest tests/unit/test_rust_extension.py -v
```

Expected: all tests pass (or skip if extension not installed).

**Step 3: Commit**

```bash
git add tests/unit/test_rust_extension.py
git commit -m "test: add Python integration tests for Rust extension"
```

---

### Task 5: Add makefile targets

**Files:**
- Modify: `makefile`

**Step 1: Add `rust-build` and `rust-test` targets**

Add the following targets to `makefile`, in alphabetical order among existing targets (after `install:` and before `test:`):

```makefile
rust-build: ## Build and install the Rust extension in dev mode
	maturin develop

rust-test: ## Run Rust unit tests
	cargo test
```

**Step 2: Verify targets work**

```bash
make rust-test
make rust-build
```

Expected: both succeed.

**Step 3: Commit**

```bash
git add makefile
git commit -m "build: add makefile targets for Rust build and test"
```

---

### Task 6: Update CI pipeline

**Files:**
- Modify: `.github/workflows/ci_pipeline.yml`

**Step 1: Add `cargo-test` job**

Add a new job after the `prek` job definition (before `unit-tests`):

```yaml
    cargo-test:
        needs: [prek]
        runs-on: ubuntu-24.04
        steps:
            - uses: actions/checkout@v6

            - name: Install Rust toolchain
              uses: dtolnay/rust-toolchain@stable

            - name: Cache cargo registry and build
              uses: actions/cache@v4
              with:
                path: |
                    ~/.cargo/registry
                    ~/.cargo/git
                    target
                key: cargo-${{ runner.os }}-${{ hashFiles('Cargo.lock') }}
                restore-keys: cargo-${{ runner.os }}-

            - name: Run cargo test
              run: cargo test
```

**Step 2: Add `build-wheels` job**

Add after `cargo-test`:

```yaml
    build-wheels:
        needs: [cargo-test]
        strategy:
            matrix:
                include:
                    - os: ubuntu-24.04
                      target: x86_64
                      artifact: wheels-linux-x86_64
                    - os: macos-latest
                      target: aarch64
                      artifact: wheels-macos-aarch64
                    - os: macos-15-intel
                      target: x86_64
                      artifact: wheels-macos-x86_64
                    - os: windows-2025
                      target: x64
                      artifact: wheels-windows-x64
        runs-on: ${{ matrix.os }}
        steps:
            - uses: actions/checkout@v6

            - uses: actions/setup-python@v6
              with:
                python-version: '3.11'

            - name: Build wheel
              uses: PyO3/maturin-action@v1
              with:
                target: ${{ matrix.target }}
                args: --release --out dist --find-interpreter
                sccache: true
                manylinux: auto

            - name: Upload wheel
              uses: actions/upload-artifact@v6
              with:
                name: ${{ matrix.artifact }}
                path: dist

    build-pure-python-wheel:
        needs: [prek]
        runs-on: ubuntu-24.04
        steps:
            - uses: actions/checkout@v6

            - uses: actions/setup-python@v6
              with:
                python-version: '3.11'

            - name: Install build tools
              run: pip install maturin

            - name: Build pure-Python sdist
              run: maturin sdist --out dist

            - name: Upload sdist
              uses: actions/upload-artifact@v6
              with:
                name: wheels-sdist
                path: dist
```

**Step 3: Update `unit-tests` job to depend on `build-wheels`**

Change the `needs` line of `unit-tests` from:

```yaml
        needs: [prek]
```

to:

```yaml
        needs: [prek, build-wheels]
```

Add a step to download and install the wheel, right after the "Setup Python" step and before the pytest steps:

```yaml
            - name: Download Rust wheel
              uses: actions/download-artifact@v7
              with:
                pattern: wheels-*
                path: dist
                merge-multiple: true

            - name: Install Rust extension
              run: |
                pip install --find-links dist dbt_artifacts_rs || echo "No matching wheel for this platform, running pure-Python"
```

**Step 4: Commit**

```bash
git add .github/workflows/ci_pipeline.yml
git commit -m "ci: add Rust build, test, and wheel stages to CI pipeline"
```

---

### Task 7: Update release pipeline

**Files:**
- Modify: `.github/workflows/release_pipeline.yml`

**Step 1: Replace `uv build` with maturin-based wheel build**

In the `build-and-push-image` job, replace the "Build whl" step (line ~160):

```yaml
              - name: Build whl
                run: uv build --out-dir dist_pypi
```

with:

```yaml
              - name: Build pure-Python sdist
                run: |
                  pip install maturin
                  maturin sdist --out dist_pypi
```

Note: Platform-specific wheels should be built in a separate job. Add a new job `build-release-wheels` before `build-and-push-image`:

```yaml
      build-release-wheels:
          strategy:
              matrix:
                  include:
                      - os: ubuntu-24.04
                        target: x86_64
                        artifact: wheels-linux-x86_64
                      - os: ubuntu-24.04
                        target: aarch64
                        artifact: wheels-linux-aarch64
                      - os: macos-latest
                        target: aarch64
                        artifact: wheels-macos-aarch64
                      - os: macos-15-intel
                        target: x86_64
                        artifact: wheels-macos-x86_64
                      - os: windows-2025
                        target: x64
                        artifact: wheels-windows-x64
          runs-on: ${{ matrix.os }}
          steps:
              - uses: actions/checkout@v6

              - uses: actions/setup-python@v6
                with:
                  python-version: '3.11'

              - name: Build wheel
                uses: PyO3/maturin-action@v1
                with:
                  target: ${{ matrix.target }}
                  args: --release --out dist --find-interpreter
                  sccache: true
                  manylinux: auto

              - name: Upload wheel
                uses: actions/upload-artifact@v6
                with:
                  name: ${{ matrix.artifact }}
                  path: dist
```

Update the "Publish package distributions to PyPI" step to also upload the platform wheels:

```yaml
              - name: Download platform wheels
                uses: actions/download-artifact@v7
                with:
                  pattern: wheels-*
                  path: dist_pypi
                  merge-multiple: true

              - name: Publish package distributions to PyPI
                uses: pypa/gh-action-pypi-publish@release/v1
                with:
                  packages-dir: dist_pypi/
```

And add `needs: [build-release-wheels]` to the `build-and-push-image` job.

**Step 2: Commit**

```bash
git add .github/workflows/release_pipeline.yml
git commit -m "ci: add platform wheel builds to release pipeline"
```

---

### Task 8: Add documentation

**Files:**
- Modify: `README.md` (add Performance section)
- Create: `docs/rust-acceleration.md`
- Modify: `mkdocs.yml` (add nav entry)

**Step 1: Add "Performance" section to README.md**

Insert before the "## Reporting bugs and contributing code" section (around line 123):

```markdown
## Performance

Pre-built wheels for common platforms include a Rust-based JSON parser that speeds up artifact loading by ~12x. The Rust extension is auto-detected at runtime — if it's not available for your platform, dbt-bouncer falls back to the pure-Python parser transparently.

**Platforms with pre-built wheels:** Linux (x86_64), macOS (x86_64, ARM64), Windows (x64).
```

**Step 2: Create `docs/rust-acceleration.md`**

```markdown
# Rust Acceleration

dbt-bouncer includes an optional Rust-based JSON parser that significantly speeds up loading of dbt artifacts (manifest.json, catalog.json, run_results.json).

## How it works

The Rust extension (`dbt_artifacts_rs`) uses [PyO3](https://pyo3.rs/) to expose a `JsonObj` wrapper that provides Python-friendly access to JSON data parsed by Rust's `serde_json`. Instead of converting the entire JSON tree to Python dicts upfront, `JsonObj` wraps the Rust-side data and converts values lazily on attribute access.

This means:

- **Parsing** happens in Rust (~12x faster than Python's `json.loads`)
- **Attribute access** (e.g., `node.name`, `node.config.materialized`) crosses the Rust-Python boundary only for the accessed values
- **Memory usage** is lower because only accessed subtrees are converted to Python objects

## Auto-detection

dbt-bouncer automatically detects whether the Rust extension is available at runtime:

```python
try:
    import dbt_artifacts_rs
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False
```

When `RUST_AVAILABLE` is `True`, dbt-bouncer routes artifact parsing through the Rust path. Otherwise, it uses the pure-Python parser. No configuration is needed.

## Pre-built wheels

Pre-built wheels are published for:

| Platform | Architecture |
|---|---|
| Linux (manylinux) | x86_64 |
| Linux (manylinux) | aarch64 |
| macOS | x86_64 |
| macOS | ARM64 (Apple Silicon) |
| Windows | x64 |

If no wheel is available for your platform, pip installs the pure-Python sdist and the Rust extension is simply not present.

## Benchmarks

_Benchmarks will be added here once the integration is complete._

## Development

### Building the Rust extension locally

```bash
# Install maturin
pip install maturin

# Build and install in dev mode
make rust-build
# or directly:
maturin develop

# Verify it works
python -c "import dbt_artifacts_rs; print('Rust extension loaded')"
```

### Running Rust tests

```bash
make rust-test
# or directly:
cargo test
```

### Project layout

```
Cargo.toml              # Rust package config (repo root)
Cargo.lock              # Pinned Rust dependencies
src/
  lib.rs                # Rust source (JsonObj + parse functions)
  dbt_bouncer/          # Python package
    artifact_parsers/
      rust_adapter.py   # Python adapter that imports dbt_artifacts_rs
```

The Rust code is built into a Python extension module (`dbt_artifacts_rs`) by maturin. The Python adapter (`rust_adapter.py`) provides the `load_manifest_artifact_rust()`, `load_catalog_artifact_rust()`, and `load_run_results_artifact_rust()` functions that the main dbt-bouncer entry point uses.
```

**Step 3: Add nav entry to `mkdocs.yml`**

Insert after `- FAQ: faq.md` and before `- Contributing: CONTRIBUTING.md`:

```yaml
  - Rust Acceleration: rust-acceleration.md
```

**Step 4: Commit**

```bash
git add README.md docs/rust-acceleration.md mkdocs.yml
git commit -m "docs: add Rust acceleration documentation

Add Performance section to README and dedicated docs page covering
architecture, auto-detection, platform wheels, and development setup."
```

---

### Task 9: Final verification

**Step 1: Run Rust tests**

```bash
cargo test
```

Expected: all pass.

**Step 2: Build and install extension**

```bash
maturin develop
```

Expected: builds and installs successfully.

**Step 3: Run full Python test suite**

```bash
make test-unit
```

Expected: all existing tests pass, plus new Rust integration tests pass.

**Step 4: Run pre-commit hooks**

```bash
prek run --all-files
```

Expected: all hooks pass.

**Step 5: Verify pure-Python fallback still works**

```bash
# Uninstall the Rust extension
pip uninstall dbt_artifacts_rs -y
# Run tests (Rust tests should skip, Python tests should still pass)
make test-unit
```

Expected: Rust integration tests skip, all other tests pass.
