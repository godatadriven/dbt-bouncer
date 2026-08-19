"""Explain command package."""

import json
from pathlib import Path
from typing import Annotated

import typer

from dbt_bouncer.cli import app
from dbt_bouncer.enums import ExitCode, ListOutputFormat


@app.command(name="explain")
def explain_check(
    check: Annotated[
        str,
        typer.Argument(
            help="Check name (e.g. 'check_model_names') or rule code (e.g. 'MO021')."
        ),
    ],
    custom_checks_dir: Annotated[
        Path | None,
        typer.Option(help="Directory containing custom checks, to explain those too."),
    ] = None,
    output_format: Annotated[
        ListOutputFormat,
        typer.Option(
            help="Format for the command output. Choices: json, text. Defaults to text.",
            case_sensitive=False,
        ),
    ] = ListOutputFormat.TEXT,
) -> None:
    """Explain a dbt-bouncer check: what it does, its parameters, and example usage.

    Works offline, no dbt artifacts required.

    Raises:
        Exit: With code CONFIG_ERROR when the check name or rule code is unknown.

    """
    import jellyfish
    from rich.console import Console

    from dbt_bouncer.cli.explain.utils import (
        build_explain_payload,
        print_text_explanation,
    )
    from dbt_bouncer.utils import get_check_registry

    registry = get_check_registry(custom_checks_dir)
    check_class = registry.get(check)

    if check_class is None:
        best_match = min(
            registry.keys(),
            key=lambda k: jellyfish.levenshtein_distance(k, check),
            default=None,
        )
        # Only offer the nearest match when it is plausibly a typo -- for
        # input that resembles nothing (e.g. 'xyzzy') a suggestion misleads.
        if (
            best_match is not None
            and jellyfish.levenshtein_distance(best_match, check) > 3
        ):
            best_match = None
        suggestion = f" Did you mean '{best_match}'?" if best_match else ""
        Console(stderr=True).print(
            f"[bold red]Unknown check name or rule code '{check}'.{suggestion}[/bold red]"
        )
        raise typer.Exit(ExitCode.CONFIG_ERROR)

    payload = build_explain_payload(check_class)

    if output_format == ListOutputFormat.JSON:
        typer.echo(json.dumps(payload, indent=2, default=str))
    else:
        print_text_explanation(payload)
