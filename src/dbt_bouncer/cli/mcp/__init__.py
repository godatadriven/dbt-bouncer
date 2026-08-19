"""MCP command package."""

import typer

from dbt_bouncer.cli import app
from dbt_bouncer.enums import ExitCode


@app.command(name="mcp")
def mcp_serve() -> None:
    """Start an MCP server that exposes dbt-bouncer to AI coding agents.

    Serves on the stdio transport. Requires the optional `mcp` dependency:
    `pip install 'dbt-bouncer[mcp]'`.

    Raises:
        Exit: With code CONFIG_ERROR when the `mcp` dependency is not installed.

    """
    try:
        from dbt_bouncer.cli.mcp.server import build_server

        server = build_server()
    except ImportError:
        typer.echo(
            "The `mcp` package is required for this command. "
            "Install it with: pip install 'dbt-bouncer[mcp]'",
            err=True,
        )
        raise typer.Exit(ExitCode.CONFIG_ERROR) from None

    server.run()
