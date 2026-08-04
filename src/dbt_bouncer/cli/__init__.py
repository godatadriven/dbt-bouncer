"""CLI commands for dbt-bouncer.

The ``app`` Typer instance lives here so that each subcommand module can
register itself with ``@app.command()`` at import time; ``main.py`` imports
the subcommand modules to trigger that registration.
"""

import typer

app = typer.Typer(
    no_args_is_help=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)

__all__ = ["app"]
