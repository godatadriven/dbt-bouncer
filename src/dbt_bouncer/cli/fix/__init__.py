"""Fix command package."""

from pathlib import Path
from typing import Annotated

import typer

from dbt_bouncer.cli import app
from dbt_bouncer.enums import ConfigFileName, ExitCode


@app.command(name="fix")
def fix(
    config_file: Annotated[
        Path | None,
        typer.Option(help="Location of the config file (YML, YAML, or TOML)."),
    ] = Path(ConfigFileName.DBT_BOUNCER_YML),
    dbt_project_dir: Annotated[
        Path | None,
        typer.Option(
            help="dbt project root, used to resolve properties-file paths. Defaults to the parent of the dbt artifacts directory."
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(help="Show what would be fixed without writing any file."),
    ] = False,
) -> None:
    """Automatically fix failures for mechanically fixable checks (experimental).

    Runs the configured checks, then edits dbt properties (YAML) files to fix
    failures whose remedy is mechanical. SQL files are never touched, and no
    properties-file entry is created — only existing entries are edited.
    Failures that need human judgement are listed as not fixable, with a
    reason. Requires the optional `fix` dependency:
    `pip install 'dbt-bouncer[fix]'`.

    Raises:
        Exit: With code SUCCESS when nothing remains to fix, CHECK_ERRORS when
            unfixable failures remain, or CONFIG_ERROR when the `ruamel.yaml`
            dependency is not installed.

    """
    try:
        import ruamel.yaml  # ruff: ignore[unused-import]
    except ImportError:
        typer.echo(
            "The `ruamel.yaml` package is required for this command. "
            "Install it with: pip install 'dbt-bouncer[fix]'",
            err=True,
        )
        raise typer.Exit(ExitCode.CONFIG_ERROR) from None

    from rich import box
    from rich.console import Console
    from rich.table import Table

    from dbt_bouncer.autofix import apply_fixes, plan_fixes
    from dbt_bouncer.cli.run.utils import prepare_run_context
    from dbt_bouncer.enums import CheckOutcome
    from dbt_bouncer.executor import Executor

    # Deliberate coupling to the runner's internal matcher: `fix` needs the
    # assembled check/resource pairs (which the public runner() discards after
    # reporting), and the benchmark suite already imports it the same way.
    from dbt_bouncer.runner import _assemble_checks_to_run

    ctx, dbt_artifacts_dir = prepare_run_context(config_file=config_file)
    project_dir = dbt_project_dir or dbt_artifacts_dir.parent

    # The executor's return value drops the check/resource references, but it
    # mutates ``checks_to_run`` in place — filter the originals, which still
    # carry the ``check`` instance and matched ``resource`` the fixers need.
    checks_to_run = _assemble_checks_to_run(ctx)
    Executor().run(checks_to_run)
    failures = [c for c in checks_to_run if c.get("outcome") == CheckOutcome.FAILED]

    console = Console()
    if not failures:
        console.print("[bold green]All checks passed — nothing to fix.[/bold green]")
        raise typer.Exit(ExitCode.SUCCESS)

    planned, skipped = plan_fixes(failures, Path(project_dir))
    applied, apply_skipped = apply_fixes(planned, dry_run=dry_run)
    skipped = skipped + apply_skipped

    if applied:
        verb = "Would fix" if dry_run else "Fixed"
        table = Table(
            title=f"[bold green]{verb}[/bold green]",
            title_justify="left",
            box=box.ROUNDED,
            border_style="green",
            show_header=True,
            header_style="bold green",
        )
        table.add_column("Check", style="cyan", no_wrap=True)
        table.add_column("File", style="magenta")
        table.add_column("Fix")
        for f in applied:
            table.add_row(f.check_run_id, str(f.file), f.description)
        console.print(table)

    if skipped:
        table = Table(
            title="[bold yellow]Not fixable[/bold yellow]",
            title_justify="left",
            box=box.ROUNDED,
            border_style="yellow",
            show_header=True,
            header_style="bold yellow",
        )
        table.add_column("Check", style="cyan", no_wrap=True)
        table.add_column("Reason")
        for s in skipped:
            table.add_row(s.check_run_id, s.reason)
        console.print(table)

    console.print(
        f"{'Would fix' if dry_run else 'Fixed'} {len(applied)} of {len(failures)} failure(s); {len(skipped)} not fixable."
    )

    if skipped or (dry_run and applied):
        raise typer.Exit(ExitCode.CHECK_ERRORS)
    raise typer.Exit(ExitCode.SUCCESS)
