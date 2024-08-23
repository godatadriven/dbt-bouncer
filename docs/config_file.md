# Config file

`dbt-bouncer` requires a config file which determines what checks are run.
The following options are available, in order of priority:

1. A file passed via the `--config=file` CLI flag.
2. A file named `dbt-bouncer.yml` in the current working directory.
3. A `[tool.dbt-bouncer]` section in `pyproject.toml`.

Here is an example config file in `yaml`:

```yaml
# [Optional] Directory where the dbt artifacts exists, generally the `target` directory inside a dbt project. Defaults to `./target`.
dbt_artifacts_dir: target

manifest_checks:
  - name: check_macro_name_matches_file_name
  - name: check_model_names
    include: ^staging
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
include = "^staging"
model_name_pattern = "^stg_"
```

For more example config files, see [here](https://github.com/godatadriven/dbt-bouncer/tree/main/tests/unit/config_files/valid).

### Common arguments

Most (but not all) checks accept the following optional arguments:

- `exclude`: Regexp to match which paths to exclude.
- `include`: Regexp to match which paths to include.

Example uses:

- `Exposures`: The path to the properties file where the source is defined, e.g. `^marts/finance` will match exposures defined in `./models/marts/finance/_exposures.yml`.
- `Macros`: The path to the macro file, e.g. `^macros/system` will match files like `./macros/system/generate_schema_name.sql`.
- `Models`: The path to the model file, e.g. `^marts` will match files like `./models/marts/customers.sql`.
- `Run Results`: The path to the file associated with the resource, e.g. `^finance/.*\.csv$` will match seeds in `./seeds/finance`, `^staging` will match models and tests in `./models/staging`.
- `Sources`: The path to the properties file where the source is defined, e.g. `^staging/crm` will match sources defined in `./models/staging/crm/_crm__sources.yml`.

To determine if a check accepts these arguments view the [Checks page](./checks/index.md).
