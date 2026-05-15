"""Check execution engine with batching and parallel execution."""

from __future__ import annotations

import logging
import threading
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn

from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError
from dbt_bouncer.enums import CheckOutcome, CheckSeverity

if TYPE_CHECKING:
    from dbt_bouncer.runner import CheckToRun

__all__ = ["Executor"]

_MAX_BATCH_SIZE: int = 500


class Executor:
    """Orchestrates check execution with batching and progress tracking."""

    def _execute_check(self, check: CheckToRun) -> CheckToRun:
        """Execute a single check and return the result.

        Returns:
            CheckToRun: The check dict with outcome and optional failure_message set.

        """
        logging.debug(f"Running {check['check_run_id']}...")
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

    def _execute_batch(self, batch: list[CheckToRun]) -> int:
        """Execute all checks in a batch sequentially and return the count.

        Returns:
            int: Number of checks executed.

        """
        for check in batch:
            self._execute_check(check)
        return len(batch)

    def run(self, checks_to_run: list[CheckToRun]) -> list[dict[str, Any]]:
        """Execute all checks with batching and progress tracking.

        Args:
            checks_to_run: List of CheckToRun dicts (mutated in place during execution).

        Returns:
            list[dict]: Result dicts with check_run_id, failure_message, outcome, severity.

        """
        if not checks_to_run:
            logging.info("No checks to run.")
            return []

        logging.info(f"Assembled {len(checks_to_run)} checks, running...")

        # Group checks by class to reduce ThreadPoolExecutor scheduling overhead.
        # Checks of the same class run sequentially within a batch; different
        # classes run in parallel across threads.
        # Cap batch size to avoid submitting one giant task for large check sets.
        batches: dict[str, list[CheckToRun]] = defaultdict(list)
        for check in checks_to_run:
            batches[check["check"].__class__.__name__].append(check)

        batches_to_run: list[list[CheckToRun]] = [
            batch[i : i + _MAX_BATCH_SIZE]
            for batch in batches.values()
            for i in range(0, len(batch), _MAX_BATCH_SIZE)
        ]

        console = Console()
        progress_lock = threading.Lock()

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Running checks...", total=len(checks_to_run))
            with ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(self._execute_batch, batch): batch
                    for batch in batches_to_run
                }
                for future in as_completed(futures):
                    batch_size = future.result()
                    with progress_lock:
                        progress.update(task, advance=batch_size)

        return [
            {
                "check_run_id": c["check_run_id"],
                "failure_message": c.get("failure_message"),
                "outcome": c["outcome"],
                "severity": c["severity"],
            }
            for c in checks_to_run
        ]
