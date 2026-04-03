"""dbt-bouncer CLI application entrypoint."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from typer.main import get_command

from dbt_bouncer.cli import init, list_checks, run, validate
from dbt_bouncer.enums import ConfigFileName, OutputFormat
from dbt_bouncer.version import version as get_version

app = typer.Typer(
    no_args_is_help=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)

app.command(name="init")(init)
app.command(name="list")(list_checks)
app.command(name="run")(run)
app.command(name="validate")(validate)


@app.callback(invoke_without_command=True)
def main_callback(
    ctx: typer.Context,
    config_file: Annotated[
        Path,
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
            help="Limit the checks run to specific check names, comma-separated. Examples: 'check_model_has_unique_test', 'check_model_names,check_source_freshness_populated'."
        ),
    ] = "",
    only: Annotated[
        str,
        typer.Option(
            help="Limit the checks run to specific categories, comma-separated. Examples: 'manifest_checks', 'catalog_checks,manifest_checks'."
        ),
    ] = "",
    output_file: Annotated[
        Path | None,
        typer.Option(help="Location of the file where check metadata will be saved."),
    ] = None,
    output_format: Annotated[
        OutputFormat,
        typer.Option(
            help="Format for the output file or stdout when no output file is specified. Choices: csv, json, junit, sarif, tap. Defaults to json.",
            case_sensitive=False,
        ),
    ] = OutputFormat.JSON,
    output_only_failures: Annotated[
        bool,
        typer.Option(
            help="If passed then only failures will be included in the output file."
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            help="Print which checks would run (name, resource type, count) without executing them.",
        ),
    ] = False,
    show_all_failures: Annotated[
        bool,
        typer.Option(
            help="If passed then all failures will be printed to the console."
        ),
    ] = False,
    verbosity: Annotated[
        int,
        typer.Option("-v", "--verbosity", help="Verbosity.", count=True),
    ] = 0,
    version: Annotated[
        bool,
        typer.Option("--version", help="Show version and exit."),
    ] = False,
) -> None:
    """Entrypoint for dbt-bouncer.

    When invoked without a subcommand, runs checks for backwards compatibility.
    Use 'dbt-bouncer run' for the explicit command.

    Raises:
        Exit: If the version flag is passed or an invalid output format is provided.

    """
    # Handle version flag
    if version:
        typer.echo(get_version())
        raise typer.Exit()

    if ctx.invoked_subcommand is None:
        ctx.invoke(
            run,
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
        )


# For mkdocs-click compatibility - export the underlying Click command
cli = get_command(app)
