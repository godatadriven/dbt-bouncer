"""Run command package."""

from pathlib import Path
from typing import Annotated

import typer

from dbt_bouncer.cli import app
from dbt_bouncer.cli.run.utils import run_bouncer
from dbt_bouncer.enums import ConfigFileName, OutputFormat


@app.command(name="run")
def run(
    config_file: Annotated[
        Path | None,
        typer.Option(help="Location of the config file (YML, YAML, or TOML)."),
    ] = Path(ConfigFileName.DBT_BOUNCER_YML),
    create_pr_comment_file: Annotated[
        bool,
        typer.Option(
            hidden=True,
            help="Create a `github-comment.md` file that will be sent to GitHub as a PR comment. Defaults to True when `dbt-bouncer` is run as a GitHub Action.",
        ),
    ] = False,
    check: Annotated[
        str,
        typer.Option(
            help="Limit the checks run to specific check names, comma-separated.",
            rich_help_panel="Check Selection",
        ),
    ] = "",
    dry_run: Annotated[
        bool,
        typer.Option(
            help="Print which checks would run (name, resource type, count) without executing them.",
            rich_help_panel="Check Selection",
        ),
    ] = False,
    only: Annotated[
        str,
        typer.Option(
            help="Limit the checks run to specific categories, comma-separated.",
            rich_help_panel="Check Selection",
        ),
    ] = "",
    output_file: Annotated[
        Path | None,
        typer.Option(
            help="Location of the file where check metadata will be saved.",
            rich_help_panel="Output Options",
        ),
    ] = None,
    output_format: Annotated[
        OutputFormat,
        typer.Option(
            help="Format for the output file or stdout when no output file is specified. Choices: csv, json, junit, sarif, tap. Defaults to json.",
            case_sensitive=False,
            rich_help_panel="Output Options",
        ),
    ] = OutputFormat.JSON,
    output_only_failures: Annotated[
        bool,
        typer.Option(
            help="If passed then only failures will be included in the output file.",
            rich_help_panel="Output Options",
        ),
    ] = False,
    show_all_failures: Annotated[
        bool,
        typer.Option(
            help="If passed then all failures will be printed to the console.",
            rich_help_panel="Display Options",
        ),
    ] = False,
    verbosity: Annotated[
        int,
        typer.Option(
            "-v",
            "--verbosity",
            help="Verbosity.",
            count=True,
            rich_help_panel="Display Options",
        ),
    ] = 0,
) -> None:
    """Run dbt-bouncer checks against your dbt project.

    [bold]Examples:[/bold]

      Run all checks with default config:
        [cyan]$ dbt-bouncer run[/cyan]

      Run specific checks only:
        [cyan]$ dbt-bouncer run --check check_model_names,check_model_has_unique_test[/cyan]

      Run manifest checks only with custom config:
        [cyan]$ dbt-bouncer run --only manifest_checks --config-file my-config.yml[/cyan]

      Save results to JSON file:
        [cyan]$ dbt-bouncer run --output-file results.json --output-format json[/cyan]

    Raises:
        Exit: If an invalid output format is provided or the checks fail.

    """
    from dbt_bouncer.cli.run.utils import _detect_config_file_source

    config_file_source = _detect_config_file_source(config_file)

    exit_code = run_bouncer(
        check=check,
        config_file=config_file,
        create_pr_comment_file=create_pr_comment_file,
        dry_run=dry_run,
        only=only,
        output_file=output_file,
        output_format=output_format,
        output_only_failures=output_only_failures,
        show_all_failures=show_all_failures,
        verbosity=verbosity,
        config_file_source=config_file_source,
    )
    raise typer.Exit(exit_code)
