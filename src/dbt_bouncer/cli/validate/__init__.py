"""Validate command package."""

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from dbt_bouncer.cli import app
from dbt_bouncer.cli.utils import resolve_config_path
from dbt_bouncer.enums import ConfigFileName
from dbt_bouncer.reporting.logger import configure_console_logging


@app.command(name="validate")
def validate(
    config_file: Annotated[
        Path | None,
        typer.Option(help="Location of the config file (YML, YAML, or TOML)."),
    ] = Path(ConfigFileName.DBT_BOUNCER_YML),
) -> None:
    """Validate the dbt-bouncer configuration file.

    Checks for YAML syntax errors and common configuration issues,
    reporting line numbers for any issues found.

    Raises:
        Exit: With code 0 if the config file is valid or with code 1 if issues are found.
        RuntimeError: If the config file is not found.

    """
    console = Console()

    configure_console_logging(verbosity=0)

    config_path = resolve_config_path(config_file)

    if not config_path.exists():
        raise RuntimeError(f"Config file not found: {config_path}")

    from dbt_bouncer.config_file_validator import lint_config_file

    issues = lint_config_file(config_path)

    if not issues:
        console.print("[bold green]Configuration file is valid![/bold green] ✅")
        raise typer.Exit(0)
    else:
        console.print(
            f"[bold red]Found {len(issues)} issue(s) in config file:[/bold red]"
        )
        for issue in issues:
            console.print(f"[red]  Line {issue['line']}: {issue['message']}[/red]")
        raise typer.Exit(1)
