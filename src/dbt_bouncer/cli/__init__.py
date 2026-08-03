"""CLI commands for dbt-bouncer.

The ``app`` Typer instance lives here so that each subcommand module can
register itself with ``@app.command()`` at import time.

``list_checks`` is re-exported lazily via ``__getattr__``, so importing this
package does not pull in the subcommand modules.

The other three subcommand functions are deliberately *not* re-exported.
``init``, ``run`` and ``validate`` are also submodule names, and importing a
submodule binds it onto its parent package — which shadows a module-level
``__getattr__``, since that is only consulted after normal attribute lookup
fails. ``main.py`` eagerly imports all four modules to trigger registration,
so in any real process ``dbt_bouncer.cli.run`` is the *module*. Re-exporting
the functions made the result depend on import order: the function on a cold
import, the module once ``dbt_bouncer.main`` had been touched.
``list_checks`` is unaffected only because no submodule shares its name.

Reach the other subcommands through their own modules instead::

    from dbt_bouncer.cli.run import run

For programmatic use prefer the supported entry point, ``run_bouncer``::

    from dbt_bouncer import run_bouncer
"""

import typer

app = typer.Typer(
    no_args_is_help=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)

__all__ = [  # ruff: ignore[undefined-export]
    "app",
    "list_checks",
]


def __getattr__(name: str):
    """Lazily import the ``list_checks`` subcommand on first access.

    Returns:
        The requested subcommand function.

    Raises:
        AttributeError: If the requested name is not a lazily-exported
            subcommand. This includes ``init``, ``run`` and ``validate``,
            which are submodules of this package rather than re-exports.

    """
    if name == "list_checks":
        from dbt_bouncer.cli.list import list_checks

        return list_checks
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
