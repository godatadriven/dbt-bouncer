"""Check execution engine with sequential execution and progress tracking."""

from __future__ import annotations

import logging
import traceback
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn

from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError
from dbt_bouncer.enums import CheckOutcome

if TYPE_CHECKING:
    from dbt_bouncer.runner import CheckToRun

__all__ = ["Executor"]


class Executor:
    """Orchestrates check execution with progress tracking."""

    # Resolved once per ``run()``. Formatting a per-check debug message costs an
    # f-string and a logging call for every check -- tens of thousands on a large
    # project -- all of it discarded when DEBUG is off, which is the default.
    _debug_enabled: bool = False

    def _execute_check(self, check: CheckToRun) -> dict[str, Any]:
        """Execute a single check and return the result dict.

        Returns:
            dict[str, Any]: Result dict with check_run_id, failure_message,
                file_path, outcome, severity, unique_id.

        """
        if self._debug_enabled:
            logging.debug(f"Running {check['check_run_id']}...")
        # Bind this run's resource onto the shared check instance immediately
        # before executing. When `_assemble_checks_to_run` pre-extracted
        # `inner_resource`, bind it directly to skip per-check unwrapping.
        chk = check["check"]
        inner = check.get("inner_resource")
        if inner is not None:
            object.__setattr__(
                chk,
                check["iterate_value"],  # ty: ignore[invalid-argument-type]
                inner,
            )
        else:
            resource = check.get("resource")
            if resource is not None:
                chk.set_resource(resource, check["iterate_value"])
        failure_message: str | None = None
        try:
            chk.execute()
            outcome = CheckOutcome.SUCCESS
        except DbtBouncerFailedCheckError as e:
            failure_message = e.message
            if chk.description:
                failure_message = f"{chk.description} - {failure_message}"

            if self._debug_enabled:
                logging.debug(
                    f"Check {check['check_run_id']} failed: {failure_message}"
                )
            outcome = CheckOutcome.FAILED
        except Exception as e:
            failure_message_full = list(
                traceback.TracebackException.from_exception(e).format(),
            )
            failure_message = failure_message_full[-1].strip()
            if self._debug_enabled:
                logging.debug(
                    f"Check {check['check_run_id']} raised unexpected error:\n{''.join(failure_message_full)}"
                )

            # Fail closed: a check that crashed has not verified anything, so it
            # keeps its configured severity (``error`` by default) rather than
            # being downgraded to a warning that would let the run pass.
            outcome = CheckOutcome.INTERNAL_ERROR
            failure_message = f"`dbt-bouncer` encountered an error ({failure_message}), run with `-v` to see more details or report an issue at https://github.com/godatadriven/dbt-bouncer/issues."
        return {
            "check_run_id": check["check_run_id"],
            "failure_message": failure_message,
            "file_path": check.get("file_path"),
            "outcome": outcome,
            "severity": check["severity"],
            "unique_id": check.get("unique_id"),
        }

    def run(self, checks_to_run: list[CheckToRun]) -> list[dict[str, Any]]:
        """Execute all checks sequentially with progress tracking.

        Args:
            checks_to_run: List of CheckToRun dicts.

        Returns:
            list[dict]: Result dicts with check_run_id, failure_message, file_path,
                outcome, severity, unique_id.

        """
        if not checks_to_run:
            logging.info("No checks to run.")
            return []

        logging.info(f"Assembled {len(checks_to_run)} checks, running...")

        self._debug_enabled = logging.getLogger().isEnabledFor(logging.DEBUG)

        # Checks are CPU-bound pure-Python work (regex matching, proxy attribute
        # access, string ops) that never releases the GIL, so a ThreadPoolExecutor
        # could not run them in parallel -- it only added thread-scheduling and
        # GIL-contention overhead (and multi-second tail-latency spikes on large
        # projects). Executing sequentially is both faster and far more
        # predictable; see the benchmark suite (``tests/benchmark``).
        console = Console()
        results: list[dict[str, Any]] = []
        append = results.append
        execute_check = self._execute_check
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
                append(execute_check(check))
                if n % update_step == 0:
                    progress.update(task, completed=n)
            progress.update(task, completed=total)

        return results
