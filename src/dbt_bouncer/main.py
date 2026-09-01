"""dbt-bouncer CLI application entrypoint."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

import dbt_bouncer.cli.explain  # ruff: ignore[unused-import] — triggers @app.command registration
import dbt_bouncer.cli.init  # ruff: ignore[unused-import] — triggers @app.command registration
import dbt_bouncer.cli.list  # ruff: ignore[unused-import] — triggers @app.command registration
import dbt_bouncer.cli.mcp  # ruff: ignore[unused-import] — triggers @app.command registration
import dbt_bouncer.cli.studio  # ruff: ignore[unused-import] — triggers @app.command registration
import dbt_bouncer.cli.validate  # ruff: ignore[unused-import] — triggers @app.command registration
from dbt_bouncer.cli import app
from dbt_bouncer.cli.run import run
from dbt_bouncer.enums import ConfigFileName, OutputFormat, PresetName
from dbt_bouncer.version import version as get_version


def _tune_gc_for_cli() -> None:
    """Raise the cyclic GC thresholds for the lifetime of a CLI run.

    Parsing a dbt project allocates a very large number of small containers (one
    proxy per manifest node, plus one per attribute access during checks). With
    CPython's default gen-0 threshold of 700 that triggers thousands of
    collections over a heap that only grows: measured on a 1,000-model project,
    the collector accounts for roughly a fifth of total runtime, nearly all of it
    in the parse and match phases.

    Only the CLI tunes this, and only here -- a library must not reconfigure the
    collector of the process that imports it, so embedders calling
    ``run_bouncer()`` keep their own settings. Collection stays enabled (rather
    than being disabled outright) so reference cycles are still reclaimed on very
    large projects; raising the thresholds recovers most of the benefit.
    """
    import gc

    gc.set_threshold(20_000, 25, 25)


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
            envvar="DBT_BOUNCER_CREATE_PR_COMMENT_FILE",
            hidden=True,
            help="Create a `github-comment.md` file that will be sent to GitHub as a PR comment. Defaults to True when `dbt-bouncer` is run as a GitHub Action.",
        ),
    ] = False,
    check: Annotated[
        str,
        typer.Option(
            envvar="DBT_BOUNCER_CHECK",
            help="Limit the checks run to specific check names, comma-separated. Examples: 'check_model_has_unique_test', 'check_model_names,check_source_freshness_populated'.",
        ),
    ] = "",
    only: Annotated[
        str,
        typer.Option(
            envvar="DBT_BOUNCER_ONLY",
            help="Limit the checks run to specific categories, comma-separated. Examples: 'manifest_checks', 'catalog_checks,manifest_checks'.",
        ),
    ] = "",
    output_file: Annotated[
        Path | None,
        typer.Option(
            envvar="DBT_BOUNCER_OUTPUT_FILE",
            help="Location of the file where check metadata will be saved.",
        ),
    ] = None,
    output_format: Annotated[
        OutputFormat,
        typer.Option(
            case_sensitive=False,
            envvar="DBT_BOUNCER_OUTPUT_FORMAT",
            help="Format for the output file (requires --output-file). Choices: csv, json, junit, sarif, tap. Defaults to json.",
        ),
    ] = OutputFormat.JSON,
    output_only_failures: Annotated[
        bool,
        typer.Option(
            envvar="DBT_BOUNCER_OUTPUT_ONLY_FAILURES",
            help="If passed then only failures will be included in the output file.",
        ),
    ] = False,
    preset: Annotated[
        PresetName | None,
        typer.Option(
            case_sensitive=False,
            envvar="DBT_BOUNCER_PRESET",
            help="Run a bundled preset config (minimal, standard, strict) instead of a config file. Ignored when --config-file is provided.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            envvar="DBT_BOUNCER_DRY_RUN",
            help="Print which checks would run (name, resource type, count) without executing them.",
        ),
    ] = False,
    show_all_failures: Annotated[
        bool,
        typer.Option(
            envvar="DBT_BOUNCER_SHOW_ALL_FAILURES",
            help="If passed then all failures will be printed to the console.",
        ),
    ] = False,
    verbosity: Annotated[
        int,
        typer.Option(
            "-v",
            "--verbosity",
            count=True,
            envvar="DBT_BOUNCER_VERBOSITY",
            help="Verbosity.",
        ),
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

    # After the early exits: `--version` and `--help` do no allocation-heavy work,
    # so they have nothing to gain from retuning the collector.
    _tune_gc_for_cli()

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
            preset=preset,
            show_all_failures=show_all_failures,
            verbosity=verbosity,
        )
