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


def _confirm_overwrite(
    console: Console, config_path: Path, *, force: bool = False
) -> None:
    """Abort unless overwriting is forced or the user agrees to overwrite.

    Raises:
        Abort: If the file exists, `force` is False, and the user declines.

    """
    if force or not config_path.exists():
        return
    console.print(
        f"\n[yellow]Warning:[/yellow] {config_path} already exists.",
        soft_wrap=True,
    )
    if not typer.confirm("Overwrite?", default=False):
        console.print("[red]Aborted.[/red]")
        raise typer.Abort()


def _validate_hint(config_path: Path) -> str:
    """Build the follow-up `dbt-bouncer validate` command hint for the created path.

    Returns:
        str: The CLI command string to validate the generated config file.

    """
    if config_path == Path(ConfigFileName.DBT_BOUNCER_YML):
        return "dbt-bouncer validate"
    return f"dbt-bouncer validate --config-file {config_path}"


def _init_from_preset(
    console: Console,
    preset: PresetName,
    *,
    force: bool = False,
    output_file: Path | None = None,
) -> None:
    """Scaffold a config file from a bundled preset, non-interactively."""
    config_path = output_file or Path(ConfigFileName.DBT_BOUNCER_YML)
    _confirm_overwrite(console, config_path, force=force)

    write_preset_config_file(preset, output_file=config_path)
    checks_count = count_preset_checks(preset)

    # `soft_wrap` stops Rich from splitting long paths or copy-pasteable commands.
    console.print(
        f"\n[bold green][OK] Created {config_path}[/bold green]",
        soft_wrap=True,
    )
    console.print(
        f"  Added the [cyan]{preset}[/cyan] preset with [cyan]{checks_count}[/cyan] checks.\n"
    )
    console.print(
        f"  Run [cyan]{_validate_hint(config_path)}[/cyan] to confirm your config is valid.\n",
        soft_wrap=True,
    )


@app.command(name="init")
def init(
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-y",
            help="Overwrite an existing config file without prompting.",
        ),
    ] = False,
    output_file: Annotated[
        Path | None,
        typer.Option(
            help=f"Where to write the config file. Defaults to `{ConfigFileName.DBT_BOUNCER_YML}`.",
        ),
    ] = None,
    preset: Annotated[
        PresetName | None,
        typer.Option(
            case_sensitive=False,
            help="Scaffold from a bundled preset (minimal, standard, strict) non-interactively.",
        ),
    ] = None,
) -> None:
    """Create a dbt-bouncer configuration file.

    Without options, asks questions to customize your initial configuration.
    With `--preset`, writes a bundled preset non-interactively.

    In both paths, `_confirm_overwrite` aborts if the target file already
    exists unless `--force` / `-y` is passed or the user confirms overwrite.
    """
    console = Console()

    if preset is not None:
        _init_from_preset(console, preset, force=force, output_file=output_file)
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

    config_path = output_file or Path(ConfigFileName.DBT_BOUNCER_YML)
    _confirm_overwrite(console, config_path, force=force)

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

    write_config_file(config_dict=result.config, output_file=config_path)

    console.print(
        f"\n[bold green][OK] Created {config_path}[/bold green]",
        soft_wrap=True,
    )
    console.print(
        f"  Added [cyan]{result.checks_count}[/cyan] checks to get you started.\n"
    )
    console.print(
        f"  Run [cyan]{_validate_hint(config_path)}[/cyan] to confirm your config is valid.\n",
        soft_wrap=True,
    )
