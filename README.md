<p align="center">
  <img src="./images/logo.webp" alt="dbt-bouncer logo" width="500"/>
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

1. Generate a `manifest.json` by running `dbt parse`.
1. Create a `dbt-bouncer.yml` config file, details [here](#config-file).
1. Run `dbt-bouncer` to validate that your conventions are being maintained. You can use GitHub Actions, Docker, a `.pex` file or python to run `dbt-bouncer`.

## GitHub Actions

```yaml
steps:
    ...

    - uses: godatadriven/dbt-bouncer@v0
      with:
        config-file: ./<PATH_TO_CONFIG_FILE>
        send-pr-comment: true # optional, defaults to true

    ...
```

## Docker

Don't use GitHub Actions? You can still use dbt-bouncer via Docker:

```bash
docker run --rm \
    --volume "$PWD":/app \
    ghcr.io/godatadriven/dbt-bouncer:vX.X.X \
    --config-file /app/<PATH_TO_CONFIG_FILE>
```

## Pex

You can also run the `.pex` ([Python EXecutable](https://docs.pex-tool.org/whatispex.html#whatispex)) artifact directly once you have a python executable installed:

```bash
wget https://github.com/godatadriven/dbt-bouncer/releases/download/vX.X.X/dbt-bouncer.pex -O dbt-bouncer.pex

dbt-bouncer.pex --config-file $PWD/<PATH_TO_CONFIG_FILE>
```

## Python

Install from [pypi.org](https://pypi.org/dbt-bouncer):

```shell
pip install dbt-bouncer
```

Run:
```shell
dbt-bouncer.pex --config-file $PWD/<PATH_TO_CONFIG_FILE>
``

# Config file

`dbt-bouncer` requires a config file to be provided. This file configures what checks are run. Here is an example config file:

```yaml
dbt-artifacts-dir: target # [Optional] Directory where the dbt artifacts exists, generally the `target` directory inside a dbt project. Defaults to `./target`.

manifest_checks:
  - name: check_macro_name_matches_file_name
  - name: check_model_names
    include: ^staging
    model_name_pattern: ^stg_
```

# Checks

:bulb: Click on a check name to see more details.

**Macros**

* [`check_macro_arguments_description_populated`](./dbt_bouncer/checks/checks.md#check_macro_arguments_description_populated): Macro arguments must have a populated description.
* [`check_macro_description_populated`](./dbt_bouncer/checks/checks.md#check_macro_description_populated): Macros must have a populated description.
* [`check_macro_name_matches_file_name`](./dbt_bouncer/checks/checks.md#check_macro_name_matches_file_name): Macros names must be the same as the file they are contained in.

**Metadata**

* [`check_project_name`](./dbt_bouncer/checks/checks.md#check_project_name): Enforce that the name of the dbt project matches a supplied regex.

**Miscellaneous**

* [`check_top_level_directories`](./dbt_bouncer/checks/checks.md#check_top_level_directories): Only specified top-level directories are allowed to contain models.

**Models**

* [`check_model_description_populated`](./dbt_bouncer/checks/checks.md#check_model_description_populated): Models must have a populated description.
* [`check_model_names`](./dbt_bouncer/checks/checks.md#check_model_names): Models must have a name that matches the supplied regex.

**Sources**

* [`check_source_has_meta_keys`](./dbt_bouncer/checks/checks.md#check_source_has_meta_keys): The `meta` config for sources must have the specified keys.

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
