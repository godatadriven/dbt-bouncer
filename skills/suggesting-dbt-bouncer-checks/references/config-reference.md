# dbt-bouncer config reference (essentials)

Config lives in `dbt-bouncer.yml` (YAML or TOML; can also live under `[tool.dbt-bouncer]` in `pyproject.toml`).

## Global options

```yaml
dbt_artifacts_dir: target        # where manifest.json etc. live (default: ./target or $DBT_PROJECT_DIR/target)
custom_checks_dir: my_checks     # optional, for Python custom checks
include: ^models/marts           # optional global path filter (regex)
exclude: ^models/staging         # optional global path filter (regex)
severity: warn                   # optional global severity — useful when onboarding an existing project
```

## Check sections

- `manifest_checks:` — needs `manifest.json` (`dbt parse`). Prefer these.
- `catalog_checks:` — needs `catalog.json` (`dbt docs generate`).
- `run_results_checks:` — needs `run_results.json` (`dbt run/build`).

## Per-check keys

Every check entry has `name` plus check-specific parameters. Common optional keys:

```yaml
- name: check_model_names
  description: Staging models must start with "stg_".   # optional, shown in failure output
  include: ^models/staging                               # regex on file path
  exclude: ^models/staging/legacy                        # regex on file path
  severity: warn                                         # warn instead of error
  model_name_pattern: ^stg_                              # check-specific parameter
```

The same check name may appear multiple times with different scopes/parameters.

## Commands

```shell
dbt parse            # produce manifest.json without warehouse access
dbt-bouncer init     # scaffold a starter config
dbt-bouncer run      # run checks; -v for verbose
```

Exit code is non-zero when any `error`-severity check fails — this is what makes it CI/pre-commit friendly.
