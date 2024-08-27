# Config file

`dbt-bouncer` requires a config file which determines what checks are run.
The following options are available, in order of priority:

1. A file passed via the `--config-file` CLI flag.
1. A file named `dbt-bouncer.yml` in the current working directory.
1. A `[tool.dbt-bouncer]` section in `pyproject.toml`.

Here is an example config file in `yaml`:

```yaml
# [Optional] Directory where the dbt artifacts exists, generally the `target` directory inside a dbt project. Defaults to `./target`.
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
# [Optional] Directory where the dbt artifacts exists, generally the `target` directory inside a dbt project. Defaults to `./target`.
dbt_artifacts_dir = "target"

[[tool.dbt-bouncer.manifest_checks]]
name = "check_macro_name_matches_file_name"

[[tool.dbt-bouncer.manifest_checks]]
name = "check_model_names"
include = "^models/staging"
model_name_pattern = "^stg_"
```

For more example config files, see [here](https://github.com/godatadriven/dbt-bouncer/tree/main/tests/unit/config_files/valid).

### Common arguments

Most (but not all) checks accept the following optional arguments:

- `exclude`: Regexp to match which original file paths to exclude.
- `include`: Regexp to match which original file paths to include.

Example per resource type:

- `Exposures`: The original file path to the properties file where the source is defined, e.g. `^models/marts/finance` will match exposures defined in `./models/marts/finance/_exposures.yml`.
- `Macros`: The original file path to the macro file, e.g. `^macros/system` will match files like `./macros/system/generate_schema_name.sql`.
- `Models`: The original file path to the model file, e.g. `^marts` will match files like `./models/marts/customers.sql`.
- `Run results`: The original file path to the file associated with the resource, e.g. `^seeds/finance` will match seeds in `./seeds/finance`, `^models/staging` will match models and tests in `./models/staging`.
- `Sources`: The original file path to the properties file where the source is defined, e.g. `^models/staging/crm` will match sources defined in `./models/staging/crm/_crm__sources.yml`.
- `Unit tests`: The original file path to the properties file where the unit test is defined, e.g. `^models/staging/crm` will match unit tests defined in `^staging/crm/_stg_crm__unit_tests.yml`.

To determine if a check accepts these arguments view the [Checks page](./checks/index.md).

!!! note

    `exclude` and `include` can be specified at both the check level and the global level. Should both levels be specified, then the check level is applied. All the below examples result in the `check_model_names` check being run on all models in `./models/staging`:

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
