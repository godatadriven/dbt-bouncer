# Config file

`dbt-bouncer` requires a config file which determines what checks are run.
The following options are available, in order of priority:

1. A file passed via the `--config-file` CLI flag.
1. A file named `dbt-bouncer.yml` in the current working directory.
1. A `[tool.dbt-bouncer]` section in `pyproject.toml`.

Here is an example config file in `yaml`:

```yaml
# [Optional] Directory where the dbt artifacts exists, generally the `target` directory inside a dbt project. Defaults to `$DBT_PROJECT_DIR/target` if $DBT_PROJECT_DIR is set, else `./target`.
dbt_artifacts_dir: target

manifest_checks:
  - name: check_macro_name_matches_file_name
  - name: check_model_names
    include: ^models/staging
    model_name_pattern: ^stg_
```

And the same config in `toml`:

```toml
[tool.dbt-bouncer]
# [Optional] Directory where the dbt artifacts exists, generally the `target` directory inside a dbt project. Defaults to `$DBT_PROJECT_DIR/target` if $DBT_PROJECT_DIR is set, else `./target`.
dbt_artifacts_dir = "target"

[[tool.dbt-bouncer.manifest_checks]]
name = "check_macro_name_matches_file_name"

[[tool.dbt-bouncer.manifest_checks]]
name = "check_model_names"
include = "^models/staging"
model_name_pattern = "^stg_"
```

For more example config files, see [here](https://github.com/godatadriven/dbt-bouncer/tree/main/tests/unit/config_files/valid). For a detailed description of how to use `dbt-bouncer` in a CI pipeline see [here](./faq.md#how-to-configure-dbt-bouncer-for-use-in-a-ci-pipeline).

## Common arguments

### Exclude and Include

Most (but not all) checks accept the following optional arguments:

- `exclude`: Regexp to match which original file paths to exclude.
- `include`: Regexp to match which original file paths to include.

Example per resource type:

- `Exposures`: The original file path to the properties file where the source is defined, e.g. `^models/marts/finance` will match exposures defined in `./models/marts/finance/_exposures.yml`.
- `Macros`: The original file path to the macro file, e.g. `^macros/system` will match files like `./macros/system/generate_schema_name.sql`.
- `Models`: The original file path to the model file, e.g. `^marts` will match files like `./models/marts/customers.sql`.
- `Run results`: The original file path to the file associated with the resource, e.g. `^seeds/finance` will match seeds in `./seeds/finance`, `^models/staging` will match models and tests in `./models/staging`.
- `Semantic models`: The original file path to the properties file where the semantic model is defined, e.g. `^models/marts/finance` will match semantic models defined in `./models/marts/finance/_finance__semantic_models.yml`.
- `Sources`: The original file path to the properties file where the source is defined, e.g. `^models/staging/crm` will match sources defined in `./models/staging/crm/_crm__sources.yml`.
- `Unit tests`: The original file path to the properties file where the unit test is defined, e.g. `^models/staging/crm` will match unit tests defined in `^staging/crm/_stg_crm__unit_tests.yml`.

To determine if a check accepts these arguments view the [Checks page](./checks/index.md).

!!! note

    `exclude` and `include` can be specified at both the check level and the global level. Should both levels be specified, then the **check** level is applied. All the below examples result in the `check_model_names` check being run on all models in `./models/staging`:

    ```yaml
    # Specify `include` at the check level only
    manifest_checks:
      - name: check_model_names
        include: ^models/staging
        model_name_pattern: ^stg_
    ```

    ```yaml
    # Specify `include` at the check and global levels
    include: ^models/marts
    manifest_checks:
      - name: check_model_names
        include: ^models/staging
        model_name_pattern: ^stg_
    ```

    ```yaml
    # Specify `include` at the global level only
    include: ^models/staging
    manifest_checks:
      - name: check_model_names
        model_name_pattern: ^stg_
    ```

!!! note

    When compiled on Windows machines, keys such as `original_file_path`, `patch_path` and `path` take the form:

    ```shell
    models\\staging\\crm\\model_1.sql
    ```

    When compiled on Linux and Mac machines, these same keys take the form:

    ```shell
    models/staging/crm/model_1.sql
    ```

    `dbt-bouncer` converts all of these paths to the Linux/Mac form, hence when you are supplying values to `exclude` and `include` you should use the Linux/Mac form.

### Only

`dbt-bouncer` has checks for three categories: catalog_checks, manifest_checks and run_results_checks. Running `dbt-bouncer` runs all checks for all categories. If you want to limit `dbt-bouncer` to a subset of check categories then you can use the `--only` CLI flag. It takes a command-separated list of check categories to run. Examples:

```shell
dbt-bouncer --only manifest_checks
dbt-bouncer --only catalog_checks,manifest_checks
```

For a detailed description see [here](./faq.md#how-to-configure-dbt-bouncer-for-use-in-a-ci-pipeline).

### Severity

All checks accept a `severity` argument, valid values are:

- `error`: If the check fails then `dbt-bouncer` will return a non-zero exit code.
- `warn`: If the check fails then `dbt-bouncer` will return a non-zero exit code.

`severity` can also be specified globally, this is useful when applying `dbt-bouncer` to a pre-existing dbt project. It allows you to run `dbt-bouncer`, identify the checks that fail and address the failures in your own time without receiving non-zero exit codes:

```yaml
# Specify `severity` at the global levels: all checks will have a `warn` severity, avoiding non-zero exit codes.
severity: warn

manifest_checks:
  - name: check_exposure_based_on_view
  ...
```

!!! note

    `severity` can be specified at both the check level and the global level. Should both levels be specified, then the **global** level is applied.

    ```yaml
    # No `severity` specified: check will have an `error` severity.
    manifest_checks:
      - name: check_exposure_based_on_view
    ```

    ```yaml
    # Specify `severity` at the check level only: check will have a `warn` severity.
    manifest_checks:
      - name: check_exposure_based_on_view
        severity: warn
    ```

    ```yaml
    # Specify `severity` at the check and global levels: check will have a `warn` severity.
    severity: warn
    manifest_checks:
      - name: check_exposure_based_on_view
        severity: error
    ```

    ```yaml
    # Specify `severity` at the global level only: check will have a `warn` severity.
    severity: warn
    manifest_checks:
      - name: check_exposure_based_on_view
    ```
