# dbt-bouncer

![CI](https://github.com/godatadriven/dbt-bouncer/actions/workflows/ci_pipeline.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Configure and enforce conventions for your dbt project.

<p align="center">
  <img src="./images/logo.webp" alt="dbt-bouncer logo" width="500"/>
</p>

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
