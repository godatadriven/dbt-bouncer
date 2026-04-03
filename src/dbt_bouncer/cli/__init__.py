"""CLI commands for dbt-bouncer.

The ``app`` Typer instance lives here so that each subcommand module can
register itself with ``@app.command()`` at import time.  Subcommand
functions are lazily re-exported via ``__getattr__`` to avoid pulling in
heavy dependencies for lightweight operations like ``--version``.
"""

import typer

app = typer.Typer(
    no_args_is_help=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)

__all__ = [  # noqa: F822
    "app",
    "init",
    "list_checks",
    "run",
    "validate",
]


def __getattr__(name: str):
    """Lazily import subcommand functions on first access.

    Returns:
        The requested subcommand function.

    Raises:
        AttributeError: If the requested name is not a valid subcommand.

    """
    if name == "init":
        from dbt_bouncer.cli.init import init

        return init
    if name == "list_checks":
        from dbt_bouncer.cli.list import list_checks

        return list_checks
    if name == "run":
        from dbt_bouncer.cli.run import run

        return run
    if name == "validate":
        from dbt_bouncer.cli.validate import validate

        return validate
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
