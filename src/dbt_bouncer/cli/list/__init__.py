"""List command package."""

import itertools
import json
from typing import Annotated

import typer

from dbt_bouncer.cli.list.utils import category_key, get_check_params
from dbt_bouncer.utils import get_check_objects


def list_checks(
    output_format: Annotated[
        str,
        typer.Option(
            help="Output format. Choices: text, json. Defaults to text.",
            case_sensitive=False,
        ),
    ] = "text",
) -> None:
    """List all available dbt-bouncer checks, grouped by category.

    Raises:
        Exit: If an invalid output format is provided.

    """
    # Map module path segment -> display category name
    category_labels = {
        "catalog": "catalog_checks",
        "manifest": "manifest_checks",
        "run_results": "run_results_checks",
    }

    if output_format.lower() not in ("text", "json"):
        typer.echo(
            f"Error: Invalid output format '{output_format}'. Choose from: text, json"
        )
        raise typer.Exit(1)

    checks = sorted(get_check_objects(), key=lambda c: (category_key(c), c.__name__))

    if output_format.lower() == "json":
        result: dict[str, list[dict[str, object]]] = {}
        for category, group in itertools.groupby(checks, key=category_key):
            label = category_labels.get(category, category)
            result[label] = []
            for check_class in group:
                docstring = (check_class.__doc__ or "").strip()
                description = docstring.splitlines()[0] if docstring else ""
                result[label].append(
                    {
                        "description": description,
                        "name": check_class.__name__,
                        "parameters": get_check_params(check_class),
                    }
                )
        typer.echo(json.dumps(result, indent=2))
    else:
        for category, group in itertools.groupby(checks, key=category_key):
            label = category_labels.get(category, category)
            typer.echo(f"{label}:")
            for check_class in group:
                docstring = (check_class.__doc__ or "").strip()
                description = docstring.splitlines()[0] if docstring else ""
                params = get_check_params(check_class)
                params_text = (
                    "\n".join(
                        f"        {name}: {type_str}"
                        for name, type_str in params.items()
                    )
                    if params
                    else "        (none)"
                )
                typer.echo(
                    f"  {check_class.__name__}:\n      {description}\n      Parameters:\n{params_text}\n"
                )
