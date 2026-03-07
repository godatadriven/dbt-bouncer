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

### The `--rust` flag

You can override auto-detection with the `--rust` flag on the `run` command:

| Value | Behaviour |
|---|---|
| `auto` (default) | Use Rust if available, fall back to Python |
| `true` | Require Rust — error if the extension is not installed |
| `false` | Force pure-Python parser, even if Rust is available |

```bash
# Default: auto-detect
dbt-bouncer run

# Force Rust (fails if extension missing)
dbt-bouncer run --rust true

# Force pure-Python
dbt-bouncer run --rust false
```

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

```text
Cargo.toml              # Rust package config (repo root)
Cargo.lock              # Pinned Rust dependencies
src/
  lib.rs                # Rust source (JsonObj + parse functions)
  dbt_bouncer/          # Python package
    artifact_parsers/
      rust_adapter.py   # Python adapter that imports dbt_artifacts_rs
```

The Rust code is built into a Python extension module (`dbt_artifacts_rs`) by maturin. The Python adapter (`rust_adapter.py`) provides the `load_manifest_artifact_rust()`, `load_catalog_artifact_rust()`, and `load_run_results_artifact_rust()` functions that the main dbt-bouncer entry point uses.
