# CLI

This page provides documentation for the `dbt-bouncer` CLI.

::: mkdocs-click
    :command: cli
    :module: dbt_bouncer.main
    :prog_name: dbt-bouncer
    :show_hidden: False
    :style: plain

## Run command

The `run` subcommand executes dbt-bouncer checks against your dbt project:

```bash
dbt-bouncer run --config-file dbt-bouncer.yml
```

This is the primary command for running checks. For backwards compatibility, `dbt-bouncer` (without the `run` subcommand) still works and behaves identically.

All the main CLI options (`--check`, `--only`, `--output-file`, etc.) work with both `dbt-bouncer run` and the legacy `dbt-bouncer` invocation.

## Validate command

The `validate` subcommand checks your configuration file for common issues:

```bash
dbt-bouncer validate --config-file dbt-bouncer.yml
```

It will report:

- YAML syntax errors with line numbers
- Missing required fields (like `name` in checks)
- Incorrect configuration types (e.g., if a check category is not a list)

Example output for a valid config:

```text
Config file is valid!
```

Example output for issues:

```text
Found 2 issue(s) in config file:
  Line 1: Check is missing required 'name' field
  Line 3: YAML syntax error: ...
```

## Exit codes

`dbt-bouncer` returns the following exit codes:

- `0`: All checks have succeeded.

- `1`:

    - At least one check has failed. Check the logs for more information.
    - A fatal error occurred during check setup or check execution. Check the logs for more information.
