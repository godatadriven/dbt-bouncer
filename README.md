# dbt-bouncer

![CI](https://github.com/godatadriven/dbt-bouncer/actions/workflows/ci_pipeline.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Configure and enforce conventions for your dbt project.

<p align="center">
  <img src="./images/logo.webp" alt="dbt-bouncer logo" width="500"/>
</p>

# How to use

1. Generate a `manifest.json` by running `dbt parse`.
1. Create a `dbt-bouncer.yml` config file.
1. Run `dbt-bouncer` to validate that your conventions are being maintained. You can use GitHub Actions, Docker or a `.pex` file to run `dbt-bouncer`.

## GitHub Actions

```yaml
steps:
    ...

    - uses: godatadriven/dbt-bouncer@v0
      with:
        config-file: ./<PATH_TO_CONFIG_FILE>

    ...
```

## Docker

Don't use GitHub Actions? You can still use dbt-bouncer via Docker:

```bash
docker pull ghcr.io/godatadriven/dbt-bouncer:v0

docker run --rm \
    --volume "$PWD/<PATH_TO_DBT_PROJECT>/target":/<PATH_TO_DBT_PROJECT>/target \
    ghcr.io/godatadriven/dbt-bouncer:v0 \
    /dbt-bouncer.pex --config-file <PATH_TO_CONFIG_FILE>
```

## Pex

You can also run the `.pex` ([Python EXecutable](https://docs.pex-tool.org/whatispex.html#whatispex)) artifact directly once you have a python executable installed:

```bash
wget https://github.com/godatadriven/dbt-bouncer/releases/download/vX.X.X/dbt-bouncer.pex -O dbt-bouncer.pex

dbt-bouncer.pex --config-file <PATH_TO_CONFIG_FILE>
```

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
make test
```
