# Frequently Asked Questions

## Can other tools perform the same checks as `dbt-bouncer`?

There are several other tools that perform similar tasks as `dbt-bouncer`.

- [dbt-checkpoint](): A collection of `pre-commit` hooks for dbt projects. Tests are written in python. Configuration is performed via `.pre-commit-config.yaml`. Provided the dbt artifacts have already been generated, `dbt-checkpoint` does not need access to the underlying database. The hooks execute when a new commit is made, as such `dbt-checkpoint` is designed to be run only as part of `pre-commit`.
- [dbt-project-evaluator](https://github.com/dbt-labs/dbt-project-evaluator): This is a dbt package from dbt Labs. Tests are written in `.sql` files using a combination of Jinja and SQL. Configuration is performed via `dbt_project.yml` and seed files (i.e. csv files). Requires a connection to underlying database. Designed to be run both in a CI pipeline and also during active development.
- [dbt-score](https://github.com/PicnicSupermarket/dbt-score): This is a python package installable via `pip`. A collection of tests that apply only to dbt models. Tests can be executed from the command line. Tests are written in python. Configuration is performed via a `pyproject.toml` file. Provided the dbt artifacts have already been generated, `dbt-score` does not need access to the underlying database. Designed to be run during development.

While the above tools inhabit the same space as `dbt-bouncer` they do not provide what we consider to be the optimum experience that `dbt-bouncer` provides:

- Designed to run both locally and in a CI pipeline.
- Configurable via a file format, `YML`, that dbt developers are already familiar with.
- Does not require database access.
- Can run tests against any of dbt's artifacts.
- Allows tests to be written in python.

As such we consider `dbt-bouncer` to be the best tool to enforce conventions in a dbt project.

!!! tip

    `dbt-bouncer` can perform all the tests currently included in `dbt-checkpoint`, `dbt-project-evaluator` and `dbt-score`. If you see an existing test that is not possible with `dbt-bouncer`, open an [issue](https://github.com/godatadriven/dbt-bouncer/issues) and we'll add it!

## How to set up `dbt-bouncer` in a monorepo?

A monorepo may consist of one directory with a dbt project and other directories with unrelated code. It may be desired for `dbt-bouncer` to be configured from the root directory. Sample directory tree:

```shell
.
├── dbt-bouncer.yml
├── README.md
├── dbt-project
│   ├── models
│   ├── dbt_project.yml
│   └── profiles.yml
└── package-a
    ├── src
    ├── tests
    └── package.json
```

To ease configuration you can use `exclude` or `include` at the global level (see [Config File](./config-file.md) for more details). For the above example `dbt-bouncer.yml` could be configured as:

```yaml
dbt_artifacts_dir: dbt-project/target
include: ^dbt-project

manifest_checks:
    - name: check_exposure_based_on_non_public_models
```

`dbt-bouncer` can now be run from the root directory.
