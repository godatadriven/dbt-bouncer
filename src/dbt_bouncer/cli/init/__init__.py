"""Init command package."""

from pathlib import Path

import typer

from dbt_bouncer.cli.init.utils import build_initial_config, write_config_file
from dbt_bouncer.enums import ConfigFileName


def init() -> None:
    """Create a dbt-bouncer.yml file interactively.

    Asks questions to customize your initial configuration.

    Raises:
        Abort: If the user declines to overwrite an existing config file.

    """
    from rich.console import Console

    console = Console()
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
    if config_path.exists():
        overwrite = typer.confirm(
            f"\n[yellow]Warning:[/yellow] {config_path} already exists. Overwrite?",
            default=False,
        )
        if not overwrite:
            console.print("[red]Aborted.[/red]")
            raise typer.Abort()

    # Build config based on answers
    config_dict, checks_count = build_initial_config(
        artifacts_dir=artifacts_dir,
        check_descriptions=check_descriptions,
        check_unique_tests=check_unique_tests,
        check_naming=check_naming,
    )

    write_config_file(config_dict=config_dict)

    console.print(f"\n[bold green][OK] Created {config_path}[/bold green]")
    console.print(f"  Added [cyan]{checks_count}[/cyan] checks to get you started.\n")
    console.print(
        "  Run [cyan]dbt-bouncer validate[/cyan] to confirm your config is valid.\n"
    )
