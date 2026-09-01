"""Init command package."""

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from dbt_bouncer.cli import app
from dbt_bouncer.cli.init.utils import (
    build_initial_config,
    count_preset_checks,
    write_config_file,
    write_preset_config_file,
)
from dbt_bouncer.enums import ConfigFileName, PresetName


def _confirm_overwrite(console: Console, config_path: Path) -> None:
    """Abort unless the user agrees to overwrite an existing config file.

    Raises:
        Abort: If the file exists and the user declines to overwrite it.

    """
    if not config_path.exists():
        return
    console.print(f"\n[yellow]Warning:[/yellow] {config_path} already exists.")
    if not typer.confirm("Overwrite?", default=False):
        console.print("[red]Aborted.[/red]")
        raise typer.Abort()


def _init_from_preset(console: Console, preset: PresetName) -> None:
    """Scaffold a config file from a bundled preset, non-interactively."""
    config_path = Path(ConfigFileName.DBT_BOUNCER_YML)
    _confirm_overwrite(console, config_path)

    write_preset_config_file(preset)
    checks_count = count_preset_checks(preset)

    console.print(f"\n[bold green][OK] Created {config_path}[/bold green]")
    console.print(
        f"  Added the [cyan]{preset}[/cyan] preset with [cyan]{checks_count}[/cyan] checks.\n"
    )
    console.print(
        "  Run [cyan]dbt-bouncer validate[/cyan] to confirm your config is valid.\n"
    )


@app.command(name="init")
def init(
    preset: Annotated[
        PresetName | None,
        typer.Option(
            case_sensitive=False,
            help="Scaffold from a bundled preset (minimal, standard, strict) non-interactively.",
        ),
    ] = None,
) -> None:
    """Create a dbt-bouncer.yml file.

    Without options, asks questions to customize your initial configuration.
    With `--preset`, writes a bundled preset non-interactively.

    In both paths, `_confirm_overwrite` aborts if the file exists and the user
    declines to overwrite it.
    """
    console = Console()

    if preset is not None:
        _init_from_preset(console, preset)
        return

    console.print("\n[bold blue]>> dbt-bouncer initialization[/bold blue]\n")

    # Interactive prompts
    artifacts_dir = typer.prompt(
        "Where are your dbt artifacts located?", default="target"
    )

    check_descriptions = typer.confirm("Check for model descriptions?", default=True)

    check_unique_tests = typer.confirm(
        "Check for unique tests on models?", default=True
    )

    check_naming = typer.confirm(
        "Check naming conventions for staging models?", default=True
    )

    config_path = Path(ConfigFileName.DBT_BOUNCER_YML)
    _confirm_overwrite(console, config_path)

    # Build config based on answers
    result = build_initial_config(
        artifacts_dir=artifacts_dir,
        check_descriptions=check_descriptions,
        check_unique_tests=check_unique_tests,
        check_naming=check_naming,
    )

    if result.checks_count == 0:
        console.print(
            "\n[yellow]Warning:[/yellow] No checks selected. Your config will be empty."
        )

    write_config_file(config_dict=result.config)

    console.print(f"\n[bold green][OK] Created {config_path}[/bold green]")
    console.print(
        f"  Added [cyan]{result.checks_count}[/cyan] checks to get you started.\n"
    )
    console.print(
        "  Run [cyan]dbt-bouncer validate[/cyan] to confirm your config is valid.\n"
    )
