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
from dbt_bouncer.enums import OutputFormatCLI
from dbt_bouncer.utils import get_check_objects


@app.command(name="list")
def list_checks(
    output_format: Annotated[
        OutputFormatCLI,
        typer.Option(
            help="Output format. Choices: text, json. Defaults to text.",
            case_sensitive=False,
        ),
    ] = OutputFormatCLI.TEXT,
) -> None:
    """List all available dbt-bouncer checks, grouped by category."""
    # Map module path segment -> display category name
    category_labels = {
        "catalog": "catalog_checks",
        "manifest": "manifest_checks",
        "run_results": "run_results_checks",
    }

    checks = sorted(get_check_objects(), key=lambda c: (category_key(c), c.__name__))
    payload = build_checks_payload(checks, category_labels)

    if output_format == OutputFormatCLI.JSON:
        typer.echo(json.dumps(payload, indent=2))
    else:
        print_text_checks(payload)
