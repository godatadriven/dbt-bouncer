# Contributing to `dbt-bouncer`

`dbt-bouncer` is open source software. Whether you are a seasoned open source contributor or a first-time committer, we welcome and encourage you to contribute code, documentation, ideas, or problem statements to this project.

## About this document

There are many ways to contribute to the ongoing development of `dbt-bouncer`, such as by participating in discussions and issues.

The rest of this document serves as a more granular guide for contributing code changes to `dbt-bouncer` (this repository). It is not intended as a guide for using `dbt-bouncer`, and some pieces assume a level of familiarity with Python development (virtualenvs, `uv`, etc). Specific code snippets in this guide assume you are using macOS or Linux and are comfortable with the command line.

If you get stuck, we're happy to help! Just open an issue or draft PR and we'll do our best to help out.

### Note

- **Branches:** All pull requests from community contributors should target the `main` branch (default).

## Getting the code

### Installing git

You will need `git` in order to download and modify the `dbt-bouncer` source code. On macOS, the best way to download git is to just install [Xcode](https://developer.apple.com/support/xcode/).

### Contributors

You can contribute to `dbt-bouncer` by forking the `dbt-bouncer` repository. For a detailed overview on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. Fork the `dbt-bouncer` repository.
1. Clone your fork locally.
1. Check out a new branch for your proposed changes.
1. Push changes to your fork.
1. Open a pull request against `godatadriven/dbt-bouncer` from your forked repository.

## Setting up an environment

The easiest way to contribute is to open this repo as a [Dev Container](https://containers.dev) in [VSCode](https://code.visualstudio.com/download) by simply clicking one of the buttons below. Everything you need will already be there!

<br />
<p align="center">
  <a href="https://vscode.dev/redirect?url=vscode://ms-vscode-remote.remote-containers/cloneInVolume?url=https://github.com/godatadriven/dbt-bouncer">
    <img
      src="https://img.shields.io/static/v1?label=Local%20Dev%20Container&message=Open&color=orange&logo=visualstudiocode&style=for-the-badge"
      alt="dbt-bouncer VSCode Dev Container"
    />
  </a>
</p>

<p align="center">
  <a href="https://github.dev/godatadriven/dbt-bouncer">
    <img
      src="https://img.shields.io/static/v1?label=GitHub%20Codespaces&message=Open&color=orange&logo=github&style=for-the-badge"
      alt="dbt-bouncer GitHub Codespaces"
    />
  </a>
</p>

Alternatively, you can follow the steps below to set up your local development environment.

There are some tools that will be helpful to you in developing locally. While this is the list relevant for `dbt-bouncer` development, many of these tools are used commonly across open-source python projects.

### Tools

These are the tools used in `dbt-bouncer` development and testing:

- [`bandit`](https://github.com/PyCQA/bandit) to check for security issues.
- [`click`](https://click.palletsprojects.com/en/8.1.x/) to create our CLI interface.
- [GitHub Actions](https://github.com/features/actions) for automating tests and checks, once a PR is pushed to the `dbt-bouncer` repository.
- [`make`](https://users.cs.duke.edu/~ola/courses/programming/Makefiles/Makefiles.html) to run multiple setup or test steps in combination.
- [`prek`](https://github.com/j178/prek) to easily run those checks.
- [`Pydantic`](https://docs.pydantic.dev/latest/) to validate our configuration file.
- [`pytest`](https://docs.pytest.org/en/latest/) to define, discover, and run tests.
- [`Ruff`](https://github.com/astral-sh/ruff) to lint and format python code.
- [`ty`](https://github.com/astral-sh/ty) for type checking.
- [`uv`](https://docs.astral.sh/uv/) to manage our python virtual environment.

A deep understanding of these tools in not required to effectively contribute to `dbt-bouncer`, but we recommend checking out the attached documentation if you're interested in learning more about each one.

#### Virtual environments

We strongly recommend using virtual environments when developing code in `dbt-bouncer`. We recommend creating this virtualenv in the root of the `dbt-bouncer` repository. To create a new virtualenv, run:

```shell
uv venv
```

This will create a new Python virtual environment.

#### Setting environment variables

Set required environment variables by copying `.env.example` to `.env` and updating the values.

## Running `dbt-bouncer` in development

### Installation

First make sure that you set up your `virtualenv` as described in [Setting up an environment](#setting-up-an-environment). Next, install `dbt-bouncer`, its dependencies and `prek`:

```shell
make install
uv run prek install
```

When installed in this way, any changes you make to your local copy of the source code will be reflected immediately in your next `dbt-bouncer` run.

### Running `dbt-bouncer`

With your virtualenv activated, the `dbt-bouncer` script should point back to the source code you've cloned on your machine. You can verify this by running `which dbt-bouncer`. This command should show you a path to an executable in your virtualenv. You can run `dbt-bouncer` using the provided example configuration file via:

```shell
uv run dbt-bouncer --config-file dbt-bouncer-example.yml
```

## Testing

Once you're able to manually test that your code change is working as expected, it's important to run existing automated tests, as well as adding some new ones. These tests will ensure that:

- Your code changes do not unexpectedly break other established functionality
- Your code changes can handle all known edge cases
- The functionality you're adding will _keep_ working in the future

### Notice

- **Generating dbt artifacts:** If you change the configuration of the dbt project located in `dbt_project` then you will need to re-generate the dbt artifacts used in testing. To do so, run:

```shell
make build-artifacts
```

### Test commands

There are a few methods for running tests locally.

#### `makefile`

There are multiple targets in the `makefile` to run common test suites, most notably:

```shell
# Runs unit tests
make test-unit

# Runs integration tests
make test-integration

# Runs all tests
make test

# Alternative for dev containers only
make test-dev-container
```

#### Performance tests

To test the performance on the `dbt-bouncer` CLI, we use [bencher](https://github.com/bencherdev/bencher) and [hyperfine](https://github.com/sharkdp/hyperfine). Provided both are installed, you can run performance tests via:

```shell
make test-perf
```

#### `prek`

[`prek`](https://github.com/j178/prek) takes care of running all code-checks for formatting and linting. Run `uv run prek install` to install `prek` in your local environment. Once this is done you can use the git pre-commit hooks to ensure proper formatting and linting.

#### `pytest`

Finally, you can also run a specific test or group of tests using [`pytest`](https://docs.pytest.org/en/latest/) directly. With a virtualenv active and dev dependencies installed you can do things like:

```shell
# run all unit tests in a file
uv run pytest ./tests/unit/checks/catalog/test_columns.py

# run a specific unit test
uv run pytest ./tests/unit/checks/catalog/test_columns.py::test_check_columns_are_documented_in_public_models
```

> See [pytest usage docs](https://docs.pytest.org/en/8.1.x/how-to/usage.html) for an overview of useful command-line options.

### Assorted development tips

- Append `# ty: ignore` to the end of a line if you need to disable `ty` on that line, preferably with the specific rule to ignore such as `# ty: ignore[invalid-argument-type]`.

## Adding a new check

There are two ways to add a check: the **decorator API** (recommended for most checks) and the **class-based API** (for checks that need pattern ABCs or complex Pydantic validation).

### Option A: Decorator API (recommended)

The `@check` decorator generates a `BaseCheck` subclass from a plain function:

```python
# src/dbt_bouncer/checks/manifest/models/naming.py
from dbt_bouncer.check_decorator import check, fail

@check("check_model_names", iterate_over="model", params={"model_name_pattern": str})
def check_model_names(model, ctx, *, model_name_pattern: str):
    """Model names must match the supplied regex."""
    import re
    if not re.match(model_name_pattern, str(model.name)):
        fail(f"`{model.unique_id}` does not match pattern `{model_name_pattern}`.")
```

**Steps:**

1. Choose the appropriate file in `./src/dbt_bouncer/checks/<category>/`.
1. Add a function decorated with `@check(name, iterate_over=..., params=...)`:
    - `name`: snake_case check name used in YAML config.
    - `iterate_over`: the resource type (`"model"`, `"source"`, `"seed"`, etc.), or omit for context-only checks.
    - `params`: dict of user-configurable parameters `{param_name: type}`. Use `(type, default)` tuple for optional params.
    - The function receives the resource as the first arg and `ctx` (CheckContext) as the second.
    - Call `fail(message)` to signal a check failure.
1. Add the check to `dbt-bouncer-example.yml` and run `dbt-bouncer --config-file dbt-bouncer-example.yml`.
1. Write tests using the test helpers:

```python
# tests/unit/checks/manifest/models/test_naming.py
from dbt_bouncer.testing import check_fails, check_passes

def test_check_model_names_pass():
    check_passes("check_model_names", model={"name": "stg_orders"}, model_name_pattern="^stg_")

def test_check_model_names_fail():
    check_fails("check_model_names", model={"name": "fct_orders"}, model_name_pattern="^stg_")
```

5. Run `make test` to ensure tests pass.
6. Open a PR!

### Option B: Class-based API

Use this when you need to inherit from a pattern ABC (see `src/dbt_bouncer/check_patterns.py`) or need complex Pydantic validation:

1. In `./src/dbt_bouncer/checks` choose the appropriate directory for your check.
1. Within the chosen file, add a Pydantic model inheriting from `BaseCheck` (or a pattern ABC):
    - Class name starts with "Check".
    - Has a `name: Literal["check_..."]` field.
    - Has an `execute()` method that raises `DbtBouncerFailedCheckError` on failure.
    - Uses `_require_*()` methods to access resources.
1. Add the check to `dbt-bouncer-example.yml` and validate.
1. Write tests using `check_passes`/`check_fails` from `dbt_bouncer.testing`.
1. Run `make test` and open a PR.

### Writing tests

The `dbt_bouncer.testing` module provides `check_passes` and `check_fails` helpers:

```python
from dbt_bouncer.testing import check_fails, check_passes

# Resource keyword arguments (model={...}) are auto-merged with sensible defaults
check_passes("check_model_names", model={"name": "stg_orders"}, model_name_pattern="^stg_")
check_fails("check_model_names", model={"name": "fct_orders"}, model_name_pattern="^stg_")

# For context-dependent checks, use ctx_* keyword arguments:
check_passes("check_model_documentation_coverage",
             min_model_documentation_coverage_pct=100,
             ctx_models=[{"description": "desc", "name": "m1", "unique_id": "model.pkg.m1"}])
```

### Writing plugins (external packages)

External packages can register checks via entry points:

```toml
# pyproject.toml of your plugin package
[project.entry-points."dbt_bouncer.checks"]
my_checks = "my_package.checks"
```

Your module can use either the `@check` decorator or class-based checks. dbt-bouncer discovers them automatically via the entry point. Use `dbt_bouncer.testing` in your own test suite.

## AI Agents and Tools

This repository includes configuration for AI coding agents. The setup follows the [AGENTS.md standard](https://agents.md):

- **`AGENTS.md`** — tool-agnostic project instructions (setup, architecture, check authoring, testing, constraints). This is the single source of truth for all AI tools.
- **`CLAUDE.md`** — Claude Code-specific configuration. References `AGENTS.md` and adds Claude-specific hooks and skills.
- **`.claude/settings.json`** — runs pre-commit hooks (`prek run --all-files`) automatically on Stop events.
- **`.claude/skills/`** — reusable skill files:
  - `/new-check` — scaffolds a new check class with tests
  - `/build-artifacts` — regenerates test fixtures after `dbt_project/` changes

**Convention:** keep `AGENTS.md` as the shared source of truth. Tool-specific files (e.g. `CLAUDE.md`, `.cursorrules`) should only add tool-specific configuration and reference `AGENTS.md` for everything else.

## Submitting a Pull Request

Code can be merged into the current development branch `main` by opening a pull request. If the proposal looks like it's on the right track, then a `dbt-bouncer` maintainer will review the PR. They may suggest code revision for style or clarity, or request that you add unit or integration test(s). These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code. Once merged, your contribution will be available for the next release of `dbt-bouncer`.

Automated tests run via GitHub Actions. If you're a first-time contributor, all tests will require a maintainer to approve.

Once all tests are passing and your PR has been approved, a `dbt-bouncer` maintainer will merge your changes into the active development branch. And that's it! Happy developing :tada:
