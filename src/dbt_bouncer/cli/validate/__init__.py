"""Validate command package."""

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from dbt_bouncer.cli import app
from dbt_bouncer.cli.run.utils import detect_config_file_source
from dbt_bouncer.enums import ConfigFileName, ExitCode
from dbt_bouncer.exceptions import DbtBouncerConfigError
from dbt_bouncer.reporting.logger import configure_console_logging


@app.command(name="validate")
def validate(
    config_file: Annotated[
        Path | None,
        typer.Option(help="Location of the config file (YML, YAML, or TOML)."),
    ] = Path(ConfigFileName.DBT_BOUNCER_YML),
) -> None:
    """Validate the dbt-bouncer configuration file.

    Checks for YAML syntax errors and common configuration issues, then runs
    the full configuration validation that `dbt-bouncer run` performs. Unknown
    keys, unknown check parameters, and mistyped values are reported with line
    numbers.

    The config file is found the same way `dbt-bouncer run` finds it: the
    `--config-file` flag, the `DBT_BOUNCER_CONFIG_FILE` environment variable,
    `dbt-bouncer.yml`, `dbt-bouncer.yaml`, `dbt-bouncer.toml`, then a
    `[tool.dbt-bouncer]` section in `pyproject.toml`.

    Raises:
        Exit: With code SUCCESS if the config file is valid, CHECK_ERRORS if issues
            are found, or CONFIG_ERROR if the config file is not found.

    """
    console = Console()

    configure_console_logging(verbosity=0)

    from dbt_bouncer.configuration_file.validator import (
        get_config_file_path,
        lint_config_file,
        lint_config_file_deep,
        load_config_file_contents,
    )

    try:
        config_path = Path(
            get_config_file_path(
                config_file=config_file or Path(ConfigFileName.DBT_BOUNCER_YML),
                config_file_source=detect_config_file_source(config_file),
            )
        )
        # A `pyproject.toml` without a `[tool.dbt-bouncer]` section means no
        # config was found, not that the config is invalid.
        if config_path.name == ConfigFileName.PYPROJECT_TOML:
            load_config_file_contents(
                config_path, allow_default_config_file_creation=False
            )
    except DbtBouncerConfigError as e:
        logging.error(str(e))
        raise typer.Exit(ExitCode.CONFIG_ERROR) from e

    issues = lint_config_file(config_path)

    # The deep pass duplicates surface findings (e.g. unknown check names), so
    # it only runs once the surface lint is clean.
    if not issues:
        issues = lint_config_file_deep(config_path)

    if not issues:
        console.print(
            f"[bold green]Configuration file is valid![/bold green] ✅ ({config_path})"
        )
        raise typer.Exit(ExitCode.SUCCESS)
    else:
        console.print(
            f"[bold red]Found {len(issues)} issue(s) in config file:[/bold red]"
        )
        for issue in issues:
            console.print(f"[red]  Line {issue['line']}: {issue['message']}[/red]")
        raise typer.Exit(ExitCode.CHECK_ERRORS)
