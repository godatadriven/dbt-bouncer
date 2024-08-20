# Config file

`dbt-bouncer` requires a config file to be provided. This file configures what checks are run. Here is an example config file:

```yaml
dbt_artifacts_dir: target # [Optional] Directory where the dbt artifacts exists, generally the `target` directory inside a dbt project. Defaults to `./target`.

manifest_checks:
  - name: check_macro_name_matches_file_name
  - name: check_model_names
    include: ^staging
    model_name_pattern: ^stg_
```

For more example config files, see [here](https://github.com/godatadriven/dbt-bouncer/tree/main/tests/unit/config_files/valid).

Note that the config can also be passed via a `pyproject.toml` file:
```yaml
[tool.dbt-bouncer]
dbt_artifacts_dir = "target"

[[tool.dbt-bouncer.manifest_checks]]
name = "check_macro_name_matches_file_name"

[[tool.dbt-bouncer.manifest_checks]]
name = "check_model_names"
include = "^staging"
model_name_pattern = "^stg_"
```
