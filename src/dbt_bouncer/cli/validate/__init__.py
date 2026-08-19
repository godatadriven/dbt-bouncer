"""Validate command package."""

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from dbt_bouncer.cli import app
from dbt_bouncer.cli.utils import resolve_config_path
from dbt_bouncer.enums import ConfigFileName, ExitCode
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

    Raises:
        Exit: With code SUCCESS if the config file is valid, CHECK_ERRORS if issues
            are found, or CONFIG_ERROR if the config file is not found.

    """
    console = Console()

    configure_console_logging(verbosity=0)

    config_path = resolve_config_path(config_file)

    if not config_path.exists():
        logging.error(f"Config file not found: {config_path}")
        raise typer.Exit(ExitCode.CONFIG_ERROR)

    from dbt_bouncer.configuration_file.validator import (
        lint_config_file,
        lint_config_file_deep,
    )

    issues = lint_config_file(config_path)

    # The deep pass duplicates surface findings (e.g. unknown check names), so
    # it only runs once the surface lint is clean.
    if not issues:
        issues = lint_config_file_deep(config_path)

    if not issues:
        console.print("[bold green]Configuration file is valid![/bold green] ✅")
        raise typer.Exit(ExitCode.SUCCESS)
    else:
        console.print(
            f"[bold red]Found {len(issues)} issue(s) in config file:[/bold red]"
        )
        for issue in issues:
            console.print(f"[red]  Line {issue['line']}: {issue['message']}[/red]")
        raise typer.Exit(ExitCode.CHECK_ERRORS)
