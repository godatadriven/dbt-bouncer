"""Baseline command package."""

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from dbt_bouncer.cli import app
from dbt_bouncer.cli.baseline.utils import DEFAULT_BASELINE_FILE, write_baseline
from dbt_bouncer.cli.run.utils import detect_config_file_source
from dbt_bouncer.enums import ConfigFileName, ExitCode
from dbt_bouncer.exceptions import DbtBouncerArtifactError, DbtBouncerConfigError


@app.command(name="baseline")
def baseline(
    config_file: Annotated[
        Path | None,
        typer.Option(help="Location of the config file (YML, YAML, or TOML)."),
    ] = Path(ConfigFileName.DBT_BOUNCER_YML),
    check: Annotated[
        str,
        typer.Option(
            envvar="DBT_BOUNCER_CHECK",
            help="Limit the checks to specific check names, comma-separated.",
        ),
    ] = "",
    only: Annotated[
        str,
        typer.Option(
            envvar="DBT_BOUNCER_ONLY",
            help="Limit the checks to specific categories, comma-separated.",
        ),
    ] = "",
    output_file: Annotated[
        Path | None,
        typer.Option(
            help=f"Where to write the baseline file. Defaults to `{DEFAULT_BASELINE_FILE}`.",
        ),
    ] = None,
    verbosity: Annotated[
        int,
        typer.Option("-v", "--verbosity", count=True, help="Verbosity."),
    ] = 0,
) -> None:
    """Record the current failures as a baseline.

    Runs every configured check and writes the failures to a baseline file.
    Commit the file, then run `dbt-bouncer run --baseline <file>` so only new
    failures are reported. Use this to adopt strict checks on a project that
    already has violations.

    Raises:
        Exit: With code SUCCESS after writing the baseline, CONFIG_ERROR if the
            config is invalid, or ARTIFACT_ERROR if a dbt artifact is missing.

    """
    console = Console()
    config_file_source = detect_config_file_source(config_file)

    try:
        count = write_baseline(
            check=check,
            config_file=config_file,
            only=only,
            output_file=output_file,
            verbosity=verbosity,
            config_file_source=config_file_source,
        )
    except DbtBouncerConfigError as e:
        logging.error(str(e))
        raise typer.Exit(ExitCode.CONFIG_ERROR) from e
    except DbtBouncerArtifactError as e:
        logging.error(str(e))
        raise typer.Exit(ExitCode.ARTIFACT_ERROR) from e

    target = output_file or Path(DEFAULT_BASELINE_FILE)
    console.print(
        f"\n[bold green][OK] Wrote {count} failure(s) to {target}[/bold green]"
    )
    console.print(
        f"  Run [cyan]dbt-bouncer run --baseline {target}[/cyan] to report only new failures.\n"
    )
    raise typer.Exit(ExitCode.SUCCESS)
