<p align="center">
  <img src="https://github.com/godatadriven/dbt-bouncer/raw/main/docs/assets/logo.svg" alt="dbt-bouncer logo" width="500"/>
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
	<img src="https://img.shields.io/badge/style-ruff-41B5BE?style=flat">
  </a>
  <a>
	<img src="https://www.aschey.tech/tokei/github/godatadriven/dbt-bouncer?category=code">
  </a>
</div>
<br/>

## Documentation

All documentation can be found on `dbt-bouncer` [documentation website](https://godatadriven.github.io/dbt-bouncer/).

### TLDR

1. Install `dbt-bouncer`:

    ```shell
    pip install dbt-bouncer
    ```

1. Create a `dbt-bouncer.yml` config file:

    ```yml
    manifest_checks:
      - name: check_model_directories
        include: ^models
        permitted_sub_directories:
          - intermediate
          - marts
          - staging
      - name: check_model_names
        include: ^models/staging
        model_name_pattern: ^stg_
    catalog_checks:
      - name: check_columns_are_documented_in_public_models
    run_results_checks:
      - name: check_run_results_max_execution_time
        max_execution_time_seconds: 60
    ```

1. Run `dbt-bouncer`:

    ```
    $ dbt-bouncer

    [...]
    Running checks... |################################| 20/20
    Done. SUCCESS=19 WARN=0 ERROR=1
    Failed checks:
    | Check name               | Severity | Failure message                                                                       |
    |--------------------------|----------|---------------------------------------------------------------------------------------|
    | check_model_directories: | error    | AssertionError: `model` is located in `utilities`, this is not a valid sub-directory. |
    ```

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Let us know and open [an issue](https://github.com/godatadriven/dbt-bouncer/issues/new/choose).
- Want to help us build `dbt-bouncer`? Check out the [Contributing Guide](https://github.com/godatadriven/dbt-bouncer/blob/HEAD/docs/CONTRIBUTING.md).

## Code of Conduct

Everyone interacting in `dbt-bouncer`'s codebase, issue trackers, chat rooms, and mailing lists is expected to follow the [Code of Conduct](./CODE_OF_CONDUCT.md).
