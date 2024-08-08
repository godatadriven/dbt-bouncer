# Contributing to `dbt-bouncer`

`dbt-bouncer` is open source software. Whether you are a seasoned open source contributor or a first-time committer, we welcome and encourage you to contribute code, documentation, ideas, or problem statements to this project.

## About this document

There are many ways to contribute to the ongoing development of `dbt-bouncer`, such as by participating in discussions and issues.

The rest of this document serves as a more granular guide for contributing code changes to `dbt-bouncer` (this repository). It is not intended as a guide for using `dbt-bouncer`, and some pieces assume a level of familiarity with Python development (virtualenvs, `Poetry`, etc). Specific code snippets in this guide assume you are using macOS or Linux and are comfortable with the command line.

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

There are some tools that will be helpful to you in developing locally. While this is the list relevant for `dbt-bouncer` development, many of these tools are used commonly across open-source python projects.

### Tools

These are the tools used in `dbt-bouncer` development and testing:

- [`black`](https://github.com/psf/black) for code formatting.
- [`click`](https://click.palletsprojects.com/en/8.1.x/) to create our CLI interface.
- [GitHub Actions](https://github.com/features/actions) for automating tests and checks, once a PR is pushed to the `dbt-bouncer` repository.
- [`make`](https://users.cs.duke.edu/~ola/courses/programming/Makefiles/Makefiles.html) to run multiple setup or test steps in combination.
- [`mypy`](https://mypy.readthedocs.io/en/stable/) for static type checking.
- [`Poetry`](https://python-poetry.org/) to manage our python virtual environment.
- [`pre-commit`](https://pre-commit.com) to easily run those checks.
- [`Pydantic`](https://docs.pydantic.dev/latest/) to validate our configuration file.
- [`pytest`](https://docs.pytest.org/en/latest/) to define, discover, and run tests.

A deep understanding of these tools in not required to effectively contribute to `dbt-bouncer`, but we recommend checking out the attached documentation if you're interested in learning more about each one.

#### Virtual environments

We strongly recommend using virtual environments when developing code in `dbt-bouncer`. We recommend creating this virtualenv in the root of the `dbt-bouncer` repository. To create a new virtualenv, run:

```shell
poetry shell
```

This will create a new Python virtual environment.

#### Setting environment variables

Set required environment variables by copying `.env.example` to `.env` and updating the values.

## Running `dbt-bouncer` in development

### Installation

First make sure that you set up your `virtualenv` as described in [Setting up an environment](#setting-up-an-environment). Next, install `dbt-bouncer`, its dependencies and `pre-commit`:

```shell
poetry install
poetry run pre-commit install
```

When installed in this way, any changes you make to your local copy of the source code will be reflected immediately in your next `dbt-bouncer` run.

### Running `dbt-bouncer`

With your virtualenv activated, the `dbt-bouncer` script should point back to the source code you've cloned on your machine. You can verify this by running `which dbt-bouncer`. This command should show you a path to an executable in your virtualenv. You can run `dbt-bouncer` using the provided example configuration file via:

```shell
dbt-bouncer --config-file dbt-bouncer-example.yml
```

## Testing

Once you're able to manually test that your code change is working as expected, it's important to run existing automated tests, as well as adding some new ones. These tests will ensure that:
- Your code changes do not unexpectedly break other established functionality
- Your code changes can handle all known edge cases
- The functionality you're adding will _keep_ working in the future

### Note

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
make test_unit

# Runs integration tests
make test_integration

# Runs all tests
make test
```

#### `pre-commit`
[`pre-commit`](https://pre-commit.com) takes care of running all code-checks for formatting and linting. Run `poetry run pre-commit install` to install `pre-commit` in your local environment. Once this is done you can use the git pre-commit hooks to ensure proper formatting and linting.

#### `pytest`

Finally, you can also run a specific test or group of tests using [`pytest`](https://docs.pytest.org/en/latest/) directly. With a virtualenv active and dev dependencies installed you can do things like:

```shell
# run all unit tests in a file
poetry run pytest ./tests/unit/checks/catalog/test_columns.py

# run a specific unit test
poetry run pytest ./tests/unit/checks/catalog/test_columns.py::test_check_columns_are_documented_in_public_models
```

> See [pytest usage docs](https://docs.pytest.org/en/8.1.x/how-to/usage.html) for an overview of useful command-line options.

### Assorted development tips

* Append `# type: ignore` to the end of a line if you need to disable `mypy` on that line.

## Submitting a Pull Request

Code can be merged into the current development branch `main` by opening a pull request. If the proposal looks like it's on the right track, then a `dbt-bouncer` maintainer will review the PR. They may suggest code revision for style or clarity, or request that you add unit or integration test(s). These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code. Once merged, your contribution will be available for the next release of `dbt-bouncer`.

Automated tests run via GitHub Actions. If you're a first-time contributor, all tests will require a maintainer to approve.

Once all tests are passing and your PR has been approved, a `dbt-bouncer` maintainer will merge your changes into the active development branch. And that's it! Happy developing :tada:
