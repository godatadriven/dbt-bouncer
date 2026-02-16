# CLI

This page provides documentation for the `dbt-bouncer` CLI.

::: mkdocs-click
    :command: cli
    :module: dbt_bouncer.main
    :prog_name: dbt-bouncer
    :show_hidden: False
    :style: plain

## Exit codes

`dbt-bouncer` returns the following exit codes:

- `0`: All checks have succeeded.

- `1`:

    - At least one check has failed. Check the logs for more information.
    - A fatal error occurred during check setup or check execution. Check the logs for more information.
