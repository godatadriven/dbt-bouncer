"""Utility functions for the studio CLI subcommand."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from dbt_bouncer.cli.explain.utils import build_explain_payload, get_check_name
from dbt_bouncer.cli.list.utils import category_key
from dbt_bouncer.version import version as get_version

if TYPE_CHECKING:
    from pathlib import Path

    from dbt_bouncer.check_framework.base import BaseCheck


def load_run_results(results_path: Path | None) -> list[dict[str, Any]]:
    """Load check run results from a JSON results file.

    Args:
        results_path: Path to the JSON results file.

    Returns:
        list[dict[str, Any]]: List of check result dictionaries.

    """
    if results_path is None or not results_path.exists():
        return []
    try:
        content = json.loads(results_path.read_text(encoding="utf-8"))
        if isinstance(content, list):
            return content
    except Exception:
        return []
    return []


def load_configured_checks(config_path: Path | None) -> set[str]:
    """Load configured check names from a dbt-bouncer config file.

    Args:
        config_path: Path to the config file (YAML, YML, or TOML).

    Returns:
        set[str]: Set of active check names configured in the file.

    """
    if config_path is None or not config_path.exists():
        return set()

    import yaml

    configured: set[str] = set()
    try:
        match config_path.suffix:
            case ".yaml" | ".yml":
                data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
            case ".toml":
                import tomllib

                data = tomllib.loads(config_path.read_text(encoding="utf-8"))
                if "tool" in data and "dbt-bouncer" in data["tool"]:
                    data = data["tool"]["dbt-bouncer"]
            case _:
                data = {}

        for key, val in data.items():
            if key.endswith("_checks") and isinstance(val, list):
                for item in val:
                    if isinstance(item, dict) and "name" in item:
                        configured.add(item["name"])
    except Exception:
        return set()

    return configured


def filter_checks(
    checks: list[type[BaseCheck]],
    *,
    category: str | None = None,
    search: str | None = None,
) -> list[type[BaseCheck]]:
    """Filter check classes by category and search term.

    Args:
        checks: List of check classes.
        category: Optional check category string.
        search: Optional search substring.

    Returns:
        list[type[BaseCheck]]: Filtered list of check classes.

    """
    filtered = checks
    if category:
        filtered = [c for c in filtered if category_key(c) == category]

    if search:
        term = search.lower().strip()
        filtered = [
            c
            for c in filtered
            if term in get_check_name(c).lower()
            or term in c.__name__.lower()
            or (getattr(c, "code", None) and term in getattr(c, "code", "").lower())
            or (c.__doc__ and term in c.__doc__.lower())
        ]

    return filtered


def render_studio_dashboard(
    checks: list[type[BaseCheck]],
    *,
    configured_checks: set[str] | None = None,
    console: Console | None = None,
    results: list[dict[str, Any]] | None = None,
    search_term: str | None = None,
    selected_category: str | None = None,
) -> None:
    """Render the rich studio dashboard to the console.

    Args:
        checks: List of check classes to display.
        configured_checks: Optional set of check names configured in the project.
        console: Optional Rich console.
        results: Optional list of run result dictionaries.
        search_term: Optional active search filter.
        selected_category: Optional active category filter.

    """
    if console is None:
        console = Console()

    configured = configured_checks or set()
    results_list = results or []

    # Map failures by check name
    failure_counts: dict[str, int] = {}
    for res in results_list:
        name = res.get("name") or res.get("check_name")
        status = res.get("status") or res.get("severity")
        if name and status in ("error", "failed", "warn"):
            failure_counts[name] = failure_counts.get(name, 0) + 1

    # Header Panel
    total_checks = len(checks)
    active_count = sum(1 for c in checks if get_check_name(c) in configured)
    header_text = Text()
    header_text.append("dbt-bouncer Studio", style="bold white on #ff694a")
    header_text.append(f"  v{get_version()}  ", style="bold white")
    header_text.append(f"• Total Checks: {total_checks} ", style="cyan")
    if configured:
        header_text.append(f"• Active in Config: {active_count} ", style="green")
    if selected_category:
        header_text.append(f"• Category: {selected_category} ", style="yellow")
    if search_term:
        header_text.append(f"• Search: '{search_term}' ", style="magenta")
    if results_list:
        total_failures = sum(failure_counts.values())
        fail_style = "bold red" if total_failures > 0 else "bold green"
        header_text.append(f"• Results: {total_failures} Failures ", style=fail_style)

    console.print(
        Panel(
            header_text,
            box=box.HEAVY,
            border_style="#ff694a",
            padding=(0, 1),
        )
    )

    if not checks:
        console.print(
            Panel(
                Text(
                    "No checks match the current search or category filter.",
                    style="yellow",
                ),
                box=box.ROUNDED,
                border_style="yellow",
            )
        )
        return

    # Check Table
    table = Table(
        title="[bold white]Available & Active Checks[/bold white]",
        title_justify="left",
        box=box.ROUNDED,
        border_style="cyan",
        header_style="bold cyan",
        expand=True,
    )
    table.add_column("Code", style="bold cyan", width=8, no_wrap=True)
    table.add_column("Check Name", style="bold white", min_width=30)
    table.add_column("Category", style="dim", width=18)
    table.add_column("Configured", justify="center", width=12)
    if results_list:
        table.add_column("Results Status", justify="center", width=16)
    table.add_column("Description", style="italic", ratio=1)

    for check_class in checks:
        code = getattr(check_class, "code", "") or "-"
        name = get_check_name(check_class)
        cat = category_key(check_class)
        is_active = name in configured
        config_badge = (
            "[bold green]Active[/bold green]" if is_active else "[dim]Available[/dim]"
        )
        doc = (
            (check_class.__doc__ or "").strip().splitlines()[0]
            if check_class.__doc__
            else ""
        )

        row_args = [code, name, cat, config_badge]
        if results_list:
            fails = failure_counts.get(name, 0)
            if fails > 0:
                res_badge = f"[bold red]{fails} Failure(s)[/bold red]"
            elif is_active:
                res_badge = "[green]Passed[/green]"
            else:
                res_badge = "[dim]Not Run[/dim]"
            row_args.append(res_badge)
        row_args.append(doc)

        table.add_row(*row_args)

    console.print(table)

    # Spotlight Panel for Top/Matched Check
    if len(checks) == 1 or (search_term and len(checks) <= 3):
        for check_class in checks:
            payload = build_explain_payload(check_class)
            detail_text = Text()
            detail_text.append(f"{payload['docstring']}\n\n", style="white")
            if payload["parameters"]:
                detail_text.append("Parameters:\n", style="bold cyan")
                for pname, pinfo in payload["parameters"].items():
                    req_label = (
                        "required"
                        if pinfo["required"]
                        else f"default: {pinfo['default']}"
                    )
                    detail_text.append(
                        f"  • {pname} ({pinfo['type']}) — {req_label}\n", style="yellow"
                    )
            if payload["documentation_url"]:
                detail_text.append(
                    f"\nDocumentation: {payload['documentation_url']}",
                    style="dim underline",
                )

            console.print(
                Panel(
                    detail_text,
                    title=f"[bold white]Spotlight: {payload['name']} ({payload['code']})[/bold white]",
                    box=box.ROUNDED,
                    border_style="green",
                )
            )
