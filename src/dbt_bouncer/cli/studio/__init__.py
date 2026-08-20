"""Studio command package."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from dbt_bouncer.cli import app
from dbt_bouncer.enums import ConfigFileName, ExitCode


@app.command(name="studio")
def studio(
    category: Annotated[
        str | None,
        typer.Option(
            "--category",
            "-c",
            help="Filter checks by category (catalog_checks, manifest_checks, run_results_checks).",
        ),
    ] = None,
    config_file: Annotated[
        Path | None,
        typer.Option(
            "--config-file",
            help="Location of the config file (YML, YAML, or TOML) to inspect active checks.",
        ),
    ] = None,
    custom_checks_dir: Annotated[
        Path | None,
        typer.Option(
            "--custom-checks-dir",
            help="Directory containing custom checks, to include those too.",
        ),
    ] = None,
    results_file: Annotated[
        Path | None,
        typer.Option(
            "--results-file",
            "-r",
            help="Path to a JSON run results file to display execution status.",
        ),
    ] = None,
    search: Annotated[
        str | None,
        typer.Option(
            "--search",
            "-s",
            help="Search checks by name, rule code, or docstring keyword.",
        ),
    ] = None,
) -> None:
    """Launch the dbt-bouncer Terminal Studio dashboard.

    Explore available and active checks, view rule parameters, and inspect run results.

    Raises:
        Exit: If an invalid category is provided.

    """
    from rich.console import Console

    from dbt_bouncer.cli.list.utils import category_key
    from dbt_bouncer.cli.studio.utils import (
        filter_checks,
        load_configured_checks,
        load_run_results,
        render_studio_dashboard,
    )
    from dbt_bouncer.enums import CheckCategory
    from dbt_bouncer.utils import get_check_objects

    valid_categories = {c.value for c in CheckCategory}
    if category and category not in valid_categories:
        Console(stderr=True).print(
            f"[bold red]Invalid category '{category}'. Choices: {', '.join(sorted(valid_categories))}[/bold red]"
        )
        raise typer.Exit(ExitCode.CONFIG_ERROR)

    checks = sorted(
        get_check_objects(custom_checks_dir=custom_checks_dir),
        key=lambda c: (category_key(c), c.__name__),
    )

    filtered = filter_checks(checks, category=category, search=search)

    # Resolve config file if provided or if default exists
    configured_checks: set[str] = set()
    if config_file and config_file.exists():
        configured_checks = load_configured_checks(config_file)
    elif config_file is None:
        for default_name in (
            ConfigFileName.DBT_BOUNCER_YML,
            ConfigFileName.DBT_BOUNCER_YAML,
            ConfigFileName.DBT_BOUNCER_TOML,
        ):
            candidate = Path(default_name)
            if candidate.exists():
                configured_checks = load_configured_checks(candidate)
                break

    results = load_run_results(results_file) if results_file else None

    render_studio_dashboard(
        filtered,
        configured_checks=configured_checks,
        results=results,
        search_term=search,
        selected_category=category,
    )
