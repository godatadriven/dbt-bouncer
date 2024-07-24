<p align="center">
  <img src="./images/logo.webp" alt="dbt-bouncer logo" width="500"/>
</p>


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
