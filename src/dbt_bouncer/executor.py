"""Check execution engine with sequential execution and progress tracking."""

from __future__ import annotations

import logging
import traceback
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn

from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError
from dbt_bouncer.enums import CheckOutcome, CheckSeverity

if TYPE_CHECKING:
    from dbt_bouncer.runner import CheckToRun

__all__ = ["Executor"]


class Executor:
    """Orchestrates check execution with progress tracking."""

    def _execute_check(self, check: CheckToRun) -> CheckToRun:
        """Execute a single check and return the result.

        Returns:
            CheckToRun: The check dict with outcome and optional failure_message set.

        """
        logging.debug(f"Running {check['check_run_id']}...")
        # Bind this run's resource onto the shared check instance immediately
        # before executing. Assembly stores the resource alongside the check
        # rather than pre-copying an instance per resource; sequential execution
        # makes reusing one instance across its resources safe. Context-only
        # checks have no resource and are executed as-is.
        resource = check.get("resource")
        if resource is not None:
            check["check"].set_resource(resource, check["iterate_value"])
        try:
            check["check"].execute()
            check["outcome"] = CheckOutcome.SUCCESS
        except DbtBouncerFailedCheckError as e:
            failure_message = e.message
            if check["check"].description:
                failure_message = f"{check['check'].description} - {failure_message}"

            logging.debug(f"Check {check['check_run_id']} failed: {failure_message}")
            check["outcome"] = CheckOutcome.FAILED
            check["failure_message"] = failure_message
        except Exception as e:
            failure_message_full = list(
                traceback.TracebackException.from_exception(e).format(),
            )
            failure_message = failure_message_full[-1].strip()
            logging.debug(
                f"Check {check['check_run_id']} raised unexpected error:\n{''.join(failure_message_full)}"
            )

            check["outcome"] = CheckOutcome.FAILED
            check["severity"] = CheckSeverity.WARN
            check["failure_message"] = (
                f"`dbt-bouncer` encountered an error ({failure_message}), run with `-v` to see more details or report an issue at https://github.com/godatadriven/dbt-bouncer/issues."
            )
        return check

    def run(self, checks_to_run: list[CheckToRun]) -> list[dict[str, Any]]:
        """Execute all checks sequentially with progress tracking.

        Args:
            checks_to_run: List of CheckToRun dicts (mutated in place during execution).

        Returns:
            list[dict]: Result dicts with check_run_id, failure_message, file_path,
                outcome, severity, unique_id.

        """
        if not checks_to_run:
            logging.info("No checks to run.")
            return []

        logging.info(f"Assembled {len(checks_to_run)} checks, running...")

        # Checks are CPU-bound pure-Python work (regex matching, proxy attribute
        # access, string ops) that never releases the GIL, so a ThreadPoolExecutor
        # could not run them in parallel -- it only added thread-scheduling and
        # GIL-contention overhead (and multi-second tail-latency spikes on large
        # projects). Executing sequentially is both faster and far more
        # predictable; see the benchmark suite (``tests/benchmark``).
        console = Console()
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            total = len(checks_to_run)
            task = progress.add_task("Running checks...", total=total)
            # Refresh the bar ~100 times total rather than once per check: at large
            # check counts per-check updates add measurable overhead, and rich only
            # renders a few times a second anyway.
            update_step = max(1, total // 100)
            for n, check in enumerate(checks_to_run, 1):
                self._execute_check(check)
                if n % update_step == 0:
                    progress.update(task, completed=n)
            progress.update(task, completed=total)

        return [
            {
                "check_run_id": c["check_run_id"],
                "failure_message": c.get("failure_message"),
                "file_path": c.get("file_path"),
                "outcome": c["outcome"],
                "severity": c["severity"],
                "unique_id": c.get("unique_id"),
            }
            for c in checks_to_run
        ]
