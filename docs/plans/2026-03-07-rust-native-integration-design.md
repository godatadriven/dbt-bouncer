# Rust Native Integration Design

**Date:** 2026-03-07
**Status:** Approved
**Branch:** feat/rust-artifact-parser

## Goal

Make the Rust-based JSON artifact parser feel native to the Python package: unified build, unified CI, tested, and documented. Ship pre-built wheels with the Rust extension baked in; fall back to pure-Python parsing on unsupported platforms.

## Context

The current Rust implementation lives in a `rust/` subdirectory as a separate project with its own `pyproject.toml`, `.gitignore`, and `.github/workflows/CI.yml`. It builds a separate PyPI package (`dbt-artifacts-rs`) that users must install independently. There are no tests, no documentation, no CI integration with the main project, and the `.github/` directory inside `rust/` is never discovered by GitHub Actions.

No existing Rust crate for parsing dbt artifacts exists as a standalone reusable library. The `dbt-schemas` crate inside dbt-labs/dbt-fusion has typed Rust structs for manifest/catalog/run_results but is deeply coupled to the dbt-fusion monorepo (~20+ workspace dependencies) and is not published to crates.io.

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Distribution model | Pre-built wheels + pure-Python fallback wheel | Matches orjson/pydantic-core pattern. No user needs Rust installed. |
| Build backend | maturin (main `pyproject.toml`) | Standard for PyO3 projects. Single build system. |
| Repo layout | `Cargo.toml` at root, Rust in `src/lib.rs` | Matches pydantic-core, orjson, watchfiles. Standard maturin layout. |
| Testing | Rust unit tests + Python integration tests | Rust tests catch low-level edge cases; Python tests validate the contract. |
| CI | Multi-stage unified workflow | cargo-test, build-wheels, test-with-rust, pure-Python fallback in one workflow. |
| Documentation | README section + docs site page | Discoverable without over-documenting. |

## Repository Layout

### Before

```
rust/
  .github/workflows/CI.yml
  .gitignore
  Cargo.toml
  pyproject.toml
  src/lib.rs
```

### After

```
Cargo.toml              # root-level
Cargo.lock              # committed
src/
  lib.rs                # Rust source
  dbt_bouncer/          # Python package (unchanged)
```

## Build System

### pyproject.toml

```toml
[build-system]
requires = ["maturin>=1.12,<2.0"]
build-backend = "maturin"

[tool.maturin]
features = ["pyo3/extension-module"]
python-source = "src"
```

### Cargo.toml

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

## CI Pipeline

### ci_pipeline.yml (modified)

New stages added before existing Python test matrix:

1. **cargo-test** - `cargo test` for Rust unit tests (~10s)
2. **build-wheels** - maturin-action builds platform wheels (linux x86_64, macOS arm64/x86_64, windows x64)
3. **test-python** (modified) - downloads built wheel, installs it, runs pytest with Rust extension active. Separate matrix entry runs without the wheel to verify pure-Python fallback.
4. **build-pure-python-wheel** - universal pure-Python wheel for unsupported platforms

### release_pipeline.yml (modified)

Build platform wheels + pure-Python wheel, upload all to PyPI.

## Testing

### Rust unit tests (src/lib.rs, #[cfg(test)] module)

- JsonObj attribute access (exact match, trailing underscore alias, missing key)
- JsonObj dict operations (items, keys, values, get, __contains__)
- JsonObj array operations (indexing, negative indexing, iteration, len)
- Primitive handling (__bool__, __eq__, __str__, __repr__)
- Enum-field special cases (access, format, resource_type kept as JsonObj for .value compat)
- value_to_py conversion (null, bool, number, string, nested objects)
- parse_manifest_json / parse_catalog_json / parse_run_results_json with valid and invalid JSON

### Python integration tests (tests/unit/test_rust_extension.py)

- Import and availability detection (RUST_AVAILABLE)
- Round-trip: parse manifest fixture with Rust, verify attribute access matches pure-Python path
- Round-trip: same for catalog and run_results
- to_python() produces native dicts/lists matching orjson.loads() output
- Edge cases: empty artifacts, missing keys, deeply nested access
- Skipped with pytest.mark.skipif(not RUST_AVAILABLE) when extension not installed

## Documentation

### README.md

Add "Performance" section:
- Pre-built wheels include Rust-based JSON parser (~12x faster artifact loading)
- Auto-detected at runtime, transparent fallback to pure-Python
- List of platforms with pre-built wheels

### docs/rust-acceleration.md

New page in mkdocs site:
- Architecture overview: what the Rust extension does and doesn't do
- How auto-detection works (rust_adapter.py tries import dbt_artifacts_rs)
- Benchmarks section (placeholder table)
- Contributor guide: maturin develop, cargo test

### makefile

New targets:
- `make rust-build` - maturin develop
- `make rust-test` - cargo test

## Change Summary

| Area | Action |
|---|---|
| `rust/` directory | Delete entirely |
| `Cargo.toml` | New file at repo root |
| `Cargo.lock` | Move to repo root |
| `src/lib.rs` | Move from `rust/src/lib.rs`, add #[cfg(test)] module |
| `.gitignore` | Add `/target` |
| `pyproject.toml` | Switch to maturin build-backend |
| `.github/workflows/ci_pipeline.yml` | Add Rust stages |
| `.github/workflows/release_pipeline.yml` | Add platform wheel builds |
| `tests/unit/test_rust_extension.py` | New file |
| `README.md` | Add "Performance" section |
| `docs/rust-acceleration.md` | New page |
| `mkdocs.yml` | Add nav entry |
| `makefile` | Add rust-build, rust-test targets |
