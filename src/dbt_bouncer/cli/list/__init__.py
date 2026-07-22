"""List command package."""

import json
from typing import Annotated

import typer

from dbt_bouncer.cli import app
from dbt_bouncer.cli.list.utils import (
    build_checks_payload,
    category_key,
    print_text_checks,
)
from dbt_bouncer.enums import CheckCategory, OutputFormatCLI
from dbt_bouncer.utils import get_check_objects


@app.command(name="list")
def list_checks(
    group: Annotated[
        CheckCategory | None,
        typer.Option(
            "--group",
            "-g",
            help="Filter checks by category group. Choices: catalog_checks, manifest_checks, run_results_checks.",
            case_sensitive=False,
        ),
    ] = None,
    output_format: Annotated[
        OutputFormatCLI,
        typer.Option(
            help="Output format. Choices: text, json. Defaults to text.",
            case_sensitive=False,
        ),
    ] = OutputFormatCLI.TEXT,
) -> None:
    """List all available dbt-bouncer checks, grouped by category."""
    category_labels = {c.directory: c.value for c in CheckCategory}

    selected_categories = frozenset({group.value}) if group is not None else None

    checks = sorted(
        get_check_objects(check_categories=selected_categories),
        key=lambda c: (category_key(c), c.__name__),
    )
    payload = build_checks_payload(checks, category_labels)

    if group is not None:
        payload = {k: v for k, v in payload.items() if k == group.value}

    if output_format == OutputFormatCLI.JSON:
        typer.echo(json.dumps(payload, indent=2))
    else:
        print_text_checks(payload)
