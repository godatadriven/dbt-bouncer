<p align="center">
  <img src="https://github.com/godatadriven/dbt-bouncer/raw/main/images/logo.webp" alt="dbt-bouncer logo" width="500"/>
</p>


<h1 align="center">
  dbt-bouncer
</h1>
<h2 align="center">
  Configure and enforce conventions for your dbt project.
</h2>

<div align="center">
  <a>
    <img src="https://img.shields.io/github/release/godatadriven/dbt-bouncer.svg?logo=github">
  </a>
  <a>
    <img src="https://github.com/godatadriven/dbt-bouncer/actions/workflows/ci_pipeline.yml/badge.svg">
  </a>
  <a>
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg">
  </a>
  <a>
    <img src="https://img.shields.io/github/last-commit/godatadriven/dbt-bouncer/main">
  </a>
  <a>
    <img src="https://img.shields.io/github/commits-since/godatadriven/dbt-bouncer/latest">
  </a>
</div>

<div align="center">
  <a>
    <img alt="dbt" src="https://img.shields.io/badge/dbt%20-%3E%3D1.6-333?logo=dbt">
  </a>
  <a>
    <img alt="Docker Supported" src="https://img.shields.io/badge/Docker%20-Supported-0db7ed?logo=docker">
  </a>
  <a>
    <img alt="GitHub Supported" src="https://img.shields.io/badge/GitHub%20-Supported-333?logo=github">
  </a>
</div>

<div align="center">
  <a>
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg">
  </a>
  <a>
    <img src="https://www.aschey.tech/tokei/github/godatadriven/dbt-bouncer?category=code">
  </a>
</div>
<br/>

# How to use

1. Generate dbt artifacts by running a dbt command.
1. Create a `dbt-bouncer.yml` config file, details [here](#config-file).
1. Run `dbt-bouncer` to validate that your conventions are being maintained. You can use GitHub Actions, Docker, a `.pex` file or python to run `dbt-bouncer`.

### GitHub Actions

```yaml
steps:
    ...

    - uses: godatadriven/dbt-bouncer@v0
      with:
        config-file: ./<PATH_TO_CONFIG_FILE>
        send-pr-comment: true # optional, defaults to true

    ...
```

### Docker

Don't use GitHub Actions? You can still use dbt-bouncer via Docker:

```bash
docker run --rm \
    --volume "$PWD":/app \
    ghcr.io/godatadriven/dbt-bouncer:vX.X.X \
    --config-file /app/<PATH_TO_CONFIG_FILE>
```

### Pex

You can also run the `.pex` ([Python EXecutable](https://docs.pex-tool.org/whatispex.html#whatispex)) artifact directly once you have a python executable (3.8 -> 3.12) installed:

```bash
wget https://github.com/godatadriven/dbt-bouncer/releases/download/vX.X.X/dbt-bouncer.pex -O dbt-bouncer.pex

python dbt-bouncer.pex --config-file $PWD/<PATH_TO_CONFIG_FILE>
```

### Python

Install from [pypi.org](https://pypi.org/p/dbt-bouncer):

```shell
pip install dbt-bouncer
```

Run:

```shell
dbt-bouncer.pex --config-file $PWD/<PATH_TO_CONFIG_FILE>
```

# Config file

`dbt-bouncer` requires a config file to be provided. This file configures what checks are run. Here is an example config file:

```yaml
dbt_artifacts_dir: target # [Optional] Directory where the dbt artifacts exists, generally the `target` directory inside a dbt project. Defaults to `./target`.

manifest_checks:
  - name: check_macro_name_matches_file_name
  - name: check_model_names
    include: ^staging
    model_name_pattern: ^stg_
```

For more example config files, see [here](https://github.com/godatadriven/dbt-bouncer/tree/main/tests/unit/config_files/valid).

Note that the config can also be passed via a `pyproject.toml` file:
```yaml
[tool.dbt-bouncer]
dbt_artifacts_dir = "target"

[[tool.dbt-bouncer.manifest_checks]]
name = "check_macro_name_matches_file_name"

[[tool.dbt-bouncer.manifest_checks]]
name = "check_model_names"
include = "^staging"
model_name_pattern = "^stg_"
```

# Checks

:bulb: Click on a check name to see more details.

### Catalog checks

These checks require the following artifact to be present:

* `catalog.json`
* `manifest.json`

**Columns**

* [`check_column_name_complies_to_column_type`](./src/dbt_bouncer/checks/checks.md#check_column_name_complies_to_column_type): Columns with specified data type must comply to the specified regexp naming pattern.

### Manifest checks

These checks require the following artifact to be present:

* `manifest.json`

**Lineage**

* [`check_lineage_permitted_upstream_models`](./src/dbt_bouncer/checks/checks.md#check_lineage_permitted_upstream_models): Upstream models must have a path that matches the provided `upstream_path_pattern`.
* [`check_lineage_seed_cannot_be_used`](./src/dbt_bouncer/checks/checks.md#check_lineage_seed_cannot_be_used): Seed cannot be referenced in models with a path that matches the specified `include` config.
* [`check_lineage_source_cannot_be_used`](./src/dbt_bouncer/checks/checks.md#check_lineage_source_cannot_be_used): Sources cannot be referenced in models with a path that matches the specified `include` config.

**Macros**

* [`check_macro_arguments_description_populated`](./src/dbt_bouncer/checks/checks.md#check_macro_arguments_description_populated): Macro arguments must have a populated description.
* [`check_macro_code_does_not_contain_regexp_pattern`](./src/dbt_bouncer/checks/checks.md#check_macro_code_does_not_contain_regexp_pattern): The raw code for a macro must not match the specified regexp pattern.
* [`check_macro_description_populated`](./src/dbt_bouncer/checks/checks.md#check_macro_description_populated): Macros must have a populated description.
* [`check_macro_name_matches_file_name`](./src/dbt_bouncer/checks/checks.md#check_macro_name_matches_file_name): Macros names must be the same as the file they are contained in.

**Metadata**

* [`check_project_name`](./src/dbt_bouncer/checks/checks.md#check_project_name): Enforce that the name of the dbt project matches a supplied regex.

**Models**

* [`check_model_access`](./src/dbt_bouncer/checks/checks.md#check_model_access): Models must have the specified access attribute.
* [`check_model_code_does_not_contain_regexp_pattern`](./src/dbt_bouncer/checks/checks.md#check_model_code_does_not_contain_regexp_pattern): The raw code for a model must not match the specified regexp pattern.
* [`check_model_description_populated`](./src/dbt_bouncer/checks/checks.md#check_model_description_populated): Models must have a populated description.
* [`check_model_directories`](./src/dbt_bouncer/checks/checks.md#check_model_directories): Only specified sub-directories are permitted.
* [`check_model_names`](./src/dbt_bouncer/checks/checks.md#check_model_names): Models must have a name that matches the supplied regex.

**Sources**

* [`check_source_has_meta_keys`](./src/dbt_bouncer/checks/checks.md#check_source_has_meta_keys): The `meta` config for sources must have the specified keys.
* [`check_source_not_orphaned`](./src/dbt_bouncer/checks/checks.md#check_source_not_orphaned): Sources must be referenced in at least one model.
* [`check_source_used_by_only_one_model`](./src/dbt_bouncer/checks/checks.md#check_source_used_by_only_one_model): Each source can be references by a maximum of one model.

**Tests**

* [`check_model_has_unique_test`](./src/dbt_bouncer/checks/checks.md#check_model_has_unique_test): Models must have a test for uniqueness of a column.

### Run results checks

These checks require the following artifact to be present:

* `manifest.json`
* `run_results.json`

**Results**

* [`check_run_results_max_gigabytes_billed`](./src/dbt_bouncer/checks/checks.md#check_run_results_max_gigabytes_billed): Each result can have a maximum number of gigabytes billed. Note that this only works for the `dbt-bigquery` adapter.
* [`check_run_results_max_execution_time`](./src/dbt_bouncer/checks/checks.md#check_run_results_max_execution_time): Each result can take a maximum duration (seconds).


# Development

To setup your development environment, fork this repository and run:

```bash
poetry shell
poetry install
pre-commit install
```

Set required environment variables by copying `.env.example` to `.env` and updating the values.

All tests can be run via:
```bash
make build-artifacts # Rebuilds dbt artifacts used by pytest
make test
```
