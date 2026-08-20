"""Utility functions for the studio CLI subcommand."""

from __future__ import annotations

import json
import logging
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

logger = logging.getLogger(__name__)


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
        logger.warning(
            "Expected results file `%s` to contain a JSON list, but got %s.",
            results_path,
            type(content).__name__,
        )
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to load results file `%s`: %s", results_path, e)
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
    except (yaml.YAMLError, OSError, Exception) as e:
        logger.warning("Failed to load configured checks from `%s`: %s", config_path, e)
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
            or term in (getattr(c, "code", "") or "").lower()
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

    active_configured = configured_checks or set()

    # Calculate failure count per check if results are provided
    failure_counts: dict[str, int] = {}
    if results:
        for r in results:
            name = r.get("check_name") or r.get("name")
            status = r.get("status")
            if name and status in ("failed", "error"):
                failure_counts[name] = failure_counts.get(name, 0) + 1

    header_text = Text()
    header_text.append("dbt-bouncer studio", style="bold red")
    header_text.append(f"  v{get_version()}", style="dim")
    if selected_category:
        header_text.append(f" | category: {selected_category}", style="cyan")
    if search_term:
        header_text.append(f" | search: '{search_term}'", style="yellow")

    total_checks = len(checks)
    active_count = sum(1 for c in checks if get_check_name(c) in active_configured)
    header_text.append(
        f"  ({total_checks} checks, {active_count} active in project)", style="green"
    )

    console.print(
        Panel(header_text, box=box.ROUNDED, style="bold white on rgb(30,30,30)")
    )

    if not checks:
        console.print(
            Panel(
                Text("No checks matched your filter criteria.", style="yellow"),
                box=box.ROUNDED,
            )
        )
        return

    table = Table(
        title="Available & Configured Checks",
        box=box.ROUNDED,
        header_style="bold magenta",
        expand=True,
    )
    table.add_column("Rule Code", style="bold cyan", width=11)
    table.add_column("Check Name", style="bold white", min_width=32)
    table.add_column("Category", style="green", width=14)
    table.add_column("Active", justify="center", width=8)
    table.add_column("Description", style="dim", ratio=1)
    if results is not None:
        table.add_column("Failures", justify="center", width=10)

    for check_class in checks:
        name = get_check_name(check_class)
        code = getattr(check_class, "code", "-") or "-"
        cat = category_key(check_class)
        is_active = name in active_configured
        active_badge = (
            Text("✓ active", style="bold green")
            if is_active
            else Text("-", style="dim")
        )

        doc = (
            (check_class.__doc__ or "").strip().split("\n")[0]
            if check_class.__doc__
            else ""
        )

        row_args = [code, name, cat, active_badge, doc]
        if results is not None:
            fails = failure_counts.get(name, 0)
            fail_badge = (
                Text(f"{fails} failed", style="bold red")
                if fails > 0
                else Text("0", style="dim green")
            )
            row_args.append(fail_badge)

        table.add_row(*row_args)

    console.print(table)

    # If a specific single check or search matches <= 3 checks, render spotlight details
    if 1 <= len(checks) <= 3:
        for check_class in checks:
            payload = build_explain_payload(check_class)
            detail_text = Text()
            detail_text.append(f"Name: {payload['name']}\n", style="bold white")
            detail_text.append(
                f"Rule Code: {payload['code'] or 'None'}\n", style="bold cyan"
            )
            detail_text.append(f"Category: {payload['category']}\n\n", style="green")
            detail_text.append(f"Description:\n{payload['docstring']}\n\n", style="dim")

            params = payload.get("parameters", {})
            if params:
                detail_text.append("Configurable Parameters:\n", style="bold underline")
                for p_name, p_info in params.items():
                    p_type = p_info.get("type", "Any")
                    p_default = p_info.get("default", "REQUIRED")
                    detail_text.append(
                        f"  • {p_name} ({p_type}): default={p_default}\n",
                        style="yellow",
                    )
            else:
                detail_text.append(
                    "Configurable Parameters: None (standard check)\n", style="dim"
                )

            if payload.get("documentation_url"):
                detail_text.append(
                    f"\nDocumentation URL: {payload['documentation_url']}\n",
                    style="blue",
                )

            console.print(
                Panel(
                    detail_text,
                    title=f"Check Details: {payload['name']}",
                    box=box.ROUNDED,
                )
            )
