"""Reporting and output formatting for check results."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from dbt_bouncer.enums import CheckOutcome, CheckSeverity
from dbt_bouncer.formatters import _format_results
from dbt_bouncer.utils import create_github_comment_file

if TYPE_CHECKING:
    from dbt_bouncer.runner import CheckToRun

__all__ = ["Reporter"]


class Reporter:
    """Handles all output formatting, progress display, and result reporting."""

    def __init__(
        self,
        *,
        show_all_failures: bool = False,
        create_pr_comment_file: bool = False,
        output_file: Path | None = None,
        output_format: str = "json",
        output_only_failures: bool = False,
    ) -> None:
        """Initialize the Reporter.

        Args:
            show_all_failures: Whether to display all failures (not just first 25).
            create_pr_comment_file: Whether to create a GitHub PR comment file.
            output_file: Path to save structured output (JSON/SARIF).
            output_format: Output format for the coverage file.
            output_only_failures: Whether to save only failed checks to the output file.

        """
        self.show_all_failures = show_all_failures
        self.create_pr_comment_file = create_pr_comment_file
        self.output_file = output_file
        self.output_format = output_format
        self.output_only_failures = output_only_failures

    def report_dry_run(
        self,
        checks_to_run: list[CheckToRun],
        *,
        iterate_cache: dict[type, frozenset[str]] | None = None,
    ) -> tuple[int, list[Any]]:
        """Print a dry-run summary table and return early.

        Args:
            checks_to_run: List of CheckToRun dicts.
            iterate_cache: Mapping of check class -> iterate-over values.
                Passed explicitly to avoid coupling to runner module state.

        Returns:
            tuple[int, list[Any]]: Always (0, []).

        """
        from collections import Counter

        if iterate_cache is None:
            iterate_cache = {}

        counts: Counter[tuple[str, str]] = Counter()
        for c in checks_to_run:
            check_name = c["check"].__class__.__name__
            resource_type = next(
                iter(iterate_cache.get(c["check"].__class__, {"(none)"})),
                "(none)",
            )
            counts[check_name, resource_type] += 1

        console = Console()
        table = Table(
            title="[bold cyan]Dry run — checks that would execute[/bold cyan]",
            title_justify="left",
            box=box.ROUNDED,
            border_style="cyan",
            show_header=True,
            header_style="bold cyan",
        )
        table.add_column("Check name", justify="left", style="cyan", no_wrap=True)
        table.add_column("Resource type", justify="center")
        table.add_column("Count", justify="right")
        for (check_name, resource_type), count in sorted(counts.items()):
            table.add_row(check_name, resource_type, str(count))
        console.print(table)
        console.print(
            Panel(
                f"[bold cyan]Dry run complete. {len(checks_to_run)} check(s) would run.[/bold cyan]",
                border_style="cyan",
            )
        )
        return 0, []

    def report_results(
        self,
        results: list[dict[str, Any]],
    ) -> tuple[int, list[dict[str, Any]]]:
        """Format and display check results, save output files.

        Args:
            results: List of result dicts with check_run_id, failure_message, outcome, severity.

        Returns:
            tuple[int, list]: Exit code (1 if errors, else 0) and the results list.

        """
        num_checks_error = 0
        num_checks_warn = 0
        num_checks_success = 0
        for r in results:
            if r["outcome"] == CheckOutcome.FAILED:
                if r["severity"] == CheckSeverity.ERROR:
                    num_checks_error += 1
                else:
                    num_checks_warn += 1
            else:
                num_checks_success += 1

        console = Console()

        if num_checks_error > 0 or num_checks_warn > 0:
            logger = logging.error if num_checks_error > 0 else logging.warning
            logger(
                f"`dbt-bouncer` {'failed' if num_checks_error > 0 else 'has warnings'}. Please see below for more details or run `dbt-bouncer` with the `-v` flag."
                + (
                    ""
                    if num_checks_error < 25 or self.show_all_failures
                    else " More than 25 checks failed, to see a full list of all failed checks re-run `dbt-bouncer` with (one of) the `--output-file` or `--show-all-failures` flags."
                )
            )
            failed_checks = [
                {
                    "check_run_id": r["check_run_id"],
                    "severity": r["severity"],
                    "failure_message": r["failure_message"],
                }
                for r in results
                if r["outcome"] == CheckOutcome.FAILED
            ]
            logging.debug(f"{failed_checks=}")

            # Set title and style based on severity
            if num_checks_error > 0:
                title = "[bold red]Failed checks[/bold red]"
                border_color = "red"
            else:
                title = "[bold yellow]Warning checks[/bold yellow]"
                border_color = "yellow"

            table = Table(
                title=title,
                title_justify="left",
                box=box.ROUNDED,
                border_style=border_color,
                show_header=True,
                header_style=f"bold {border_color}",
            )
            table.add_column("Check name", justify="left", style="cyan", no_wrap=True)
            table.add_column("Severity", justify="center", width=10)
            table.add_column("Failure message", justify="left")

            checks_to_display = (
                failed_checks if self.show_all_failures else failed_checks[:25]
            )

            for check in checks_to_display:
                # Determine color based on severity string
                sev = str(check.get("severity", "")).lower()
                sev_color = "red" if CheckSeverity.ERROR in sev else "yellow"

                table.add_row(
                    str(check.get("check_run_id", "")),
                    f"[bold {sev_color}]{sev.upper()}[/bold {sev_color}]",
                    str(check.get("failure_message", "")),
                )

            console.print(table)

            if self.create_pr_comment_file:
                create_github_comment_file(
                    failed_checks=[
                        [str(f["check_run_id"]), str(f.get("failure_message", ""))]
                        for f in failed_checks
                    ],
                    show_all_failures=self.show_all_failures,
                )

        if num_checks_error == 0 and num_checks_warn == 0:
            console.print(
                Panel(
                    f"[bold green][OK] All checks passed! SUCCESS={num_checks_success} WARN={num_checks_warn} ERROR={num_checks_error}[/bold green]",
                    border_style="green",
                )
            )
        else:
            console.print(
                f"Done. [bold green]SUCCESS={num_checks_success}[/bold green] "
                f"[bold yellow]WARN={num_checks_warn}[/bold yellow] "
                f"[bold red]ERROR={num_checks_error}[/bold red]"
            )

        results_to_save = (
            [r for r in results if r["outcome"] == CheckOutcome.FAILED]
            if self.output_only_failures
            else results
        )

        if self.output_file is not None:
            coverage_file = Path().cwd() / self.output_file
            logging.info(f"Saving coverage file to `{coverage_file}`.")
            coverage_file.write_bytes(
                _format_results(results_to_save, self.output_format)
            )

        return 1 if num_checks_error != 0 else 0, results
