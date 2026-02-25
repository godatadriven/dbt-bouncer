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
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg">
  </a>
  <a>
    <img alt="security: bandit" href="https://github.com/PyCQA/bandit" src="https://img.shields.io/badge/security-bandit-yellow.svg">
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
    <img alt="dbt-core" src="https://img.shields.io/badge/dbt--core%20-%3E%3D1.7-333?logo=dbt">
  </a>
  <a>
    <img alt="dbt Cloud Supported" src="https://img.shields.io/badge/dbt%20Cloud%20-Supported-ff694a?logo=dbt">
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
    <img src="https://github.com/godatadriven/dbt-bouncer/actions/workflows/merge_pipeline.yml/badge.svg">
  </a>
  <a>
    <img src="https://github.com/godatadriven/dbt-bouncer/actions/workflows/post_release_pipeline.yml/badge.svg">
  </a>
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

1. `dbt-bouncer` requires a `manifest.json` file. If not already present, run:

    ```shell
    dbt parse
    ```

1. Create a `dbt-bouncer.yml` config file:

    ```shell
    dbt-bouncer init
    ```

1. Run `dbt-bouncer`:

    ```text
    $ dbt-bouncer run
    Running dbt-bouncer (X.X.X)...
    Loaded config from dbt-bouncer-example.yml...
    Validating conf...
    Parsed artifacts for `dbt_bouncer_test_project` project:
    ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━┓
    ┃ Artifact      ┃ Category        ┃ Count ┃
    ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━┩
    │ manifest.json │ Exposures       │     2 │
    │               │ Macros          │     3 │
    │               │ Nodes           │    12 │
    │               │ Seeds           │     3 │
    │               │ Semantic Models │     1 │
    │               │ Snapshots       │     2 │
    │               │ Sources         │     4 │
    │               │ Tests           │    36 │
    │               │ Unit Tests      │     3 │
    └───────────────┴─────────────────┴───────┘
    Assembled 366 checks, running...
    Running checks... ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100%
    `dbt-bouncer` failed. Please see below for more details or run `dbt-bouncer` with the `-v` flag.
    Failed checks:
    Check name                          Severity  Failure message
    check_model_names:48:orders         error     Models in the staging layer should always start with "stg_". - `stg_orders` does not match the supplied regex `^stg_`.
    Done. SUCCESS=365 WARN=0 ERROR=1
    ```

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Let us know and open [an issue](https://github.com/godatadriven/dbt-bouncer/issues/new/choose).
- Want to help us build `dbt-bouncer`? Check out the [Contributing Guide](https://github.com/godatadriven/dbt-bouncer/blob/HEAD/docs/CONTRIBUTING.md).

## Code of Conduct

Everyone interacting in `dbt-bouncer`'s codebase, issue trackers, chat rooms, and mailing lists is expected to follow the [Code of Conduct](./CODE_OF_CONDUCT.md).
