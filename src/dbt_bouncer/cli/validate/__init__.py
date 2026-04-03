"""Validate command package."""

import logging
from pathlib import Path
from typing import Annotated

import typer

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
        Exit: If the config file is valid or if issues are found.
        RuntimeError: If the config file is not found.

    """
    configure_console_logging(verbosity=0)

    config_path = resolve_config_path(config_file)

    if not config_path.exists():
        raise RuntimeError(f"Config file not found: {config_path}")

    from dbt_bouncer.config_file_validator import lint_config_file

    issues = lint_config_file(config_path)

    if not issues:
        logging.info("Config file is valid!")
        raise typer.Exit(0)
    else:
        logging.error(f"Found {len(issues)} issue(s) in config file:")
        for issue in issues:
            logging.error(f"  Line {issue['line']}: {issue['message']}")
        raise typer.Exit(1)
