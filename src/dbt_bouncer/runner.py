"""Assemble and run all checks."""

import logging
import operator
import threading
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn
from rich.table import Table

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.formatters import _format_results
from dbt_bouncer.resource_type import ResourceType
from dbt_bouncer.utils import (
    create_github_comment_file,
    get_nested_value,
    resource_in_path,
)

if TYPE_CHECKING:
    from dbt_bouncer.context import BouncerContext


_MAX_BATCH_SIZE: int = 500
_VALID_ITERATE_OVER_VALUES = frozenset(rt.value for rt in ResourceType)
_CLASS_ITERATE_CACHE: dict[type, frozenset[str]] = {}


def _get_resource_meta(
    resource: Any,
    iterate_value: str,
    meta_by_unique_id: dict[str, Any],
) -> dict[str, Any]:
    """Extract the dbt-bouncer meta config for a resource.

    Different resource types store their meta config in different locations.
    This helper centralises that per-type logic.

    Args:
        resource: The wrapper resource object (e.g. DbtBouncerModel).
        iterate_value: The singular resource type name (e.g. "model", "test").
        meta_by_unique_id: Pre-built mapping of unique_id -> meta for catalog nodes.

    Returns:
        dict[str, Any]: The meta dict for the resource, or {} if not applicable.

    """
    if iterate_value in {"model", "seed", "semantic_model", "snapshot", "source"}:
        try:
            return getattr(resource, iterate_value).config.meta or {}
        except AttributeError:
            return getattr(resource, iterate_value).meta or {}
    elif iterate_value == "catalog_node":
        return meta_by_unique_id.get(getattr(resource, "unique_id", ""), {})
    elif iterate_value == "run_result":
        return {}
    elif iterate_value == "macro":
        return resource.meta or {}
    elif iterate_value == "test":
        return getattr(getattr(resource, "test", resource), "meta", {}) or {}
    else:
        try:
            return resource.config.meta or {}
        except AttributeError:
            return resource.meta or {}


def _build_check_run_id(check: Any, resource: Any, iterate_value: str) -> str:
    """Build a unique run ID string for a check against a specific resource.

    Args:
        check: The check instance (must have .name and .index attributes).
        resource: The wrapper resource object.
        iterate_value: The singular resource type name (e.g. "model", "exposure").

    Returns:
        str: The check run ID in the format "check_name:index:resource_suffix".

    """
    match iterate_value:
        case "exposure" | "macro" | "test" | "unit_test":
            suffix = resource.unique_id.split(".")[-1]
        case _:
            suffix = "_".join(getattr(resource, iterate_value).unique_id.split(".")[2:])
    return f"{check.name}:{check.index}:{suffix}"


class CheckToRun(TypedDict):
    """A single check instance ready for execution, with its run context."""

    check: Any
    check_run_id: str
    failure_message: NotRequired[list[str] | str]
    outcome: NotRequired[str]
    severity: str


def _should_run_check(
    check: Any,
    resource: Any,
    iterate_over_value: frozenset[str],
    meta_config: list[str],
) -> bool:
    """Determine if a check should run against a given resource.

    Evaluates three conditions:
    1. The resource path matches the check's include/exclude patterns.
    2. For model checks, the materialization matches (if specified).
    3. The check is not listed in the resource's skip_checks meta config.

    Returns:
        bool: Whether the check should run.

    """
    if not resource_in_path(check, resource):
        return False

    if (
        iterate_over_value == {"model"}
        and check.materialization is not None
        and check.materialization != resource.model.config.materialized
    ):
        return False

    return not (meta_config and check.name in meta_config)


def runner(
    ctx: "BouncerContext",
) -> tuple[int, list[Any]]:
    """Run dbt-bouncer checks.

    Returns:
        tuple[int, list[Any]]: A tuple containing the exit code and a list of failed checks.

    Raises:
        RuntimeError: If more than one "iterate_over" argument is found.

    """
    parsed_data = {
        "catalog_nodes": ctx.catalog_nodes,
        "catalog_sources": ctx.catalog_sources,
        "exposures": ctx.exposures,
        "macros": ctx.macros,
        "manifest_obj": ctx.manifest_obj,
        "models": [m.model for m in ctx.models],
        "models_by_unique_id": {m.model.unique_id: m.model for m in ctx.models},
        "sources_by_unique_id": {s.source.unique_id: s.source for s in ctx.sources},
        "exposures_by_unique_id": {e.unique_id: e for e in ctx.exposures},
        "tests_by_unique_id": {t.test.unique_id: t.test for t in ctx.tests},
        "run_results": [r.run_result for r in ctx.run_results],
        "seeds": [s.seed for s in ctx.seeds],
        "semantic_models": [s.semantic_model for s in ctx.semantic_models],
        "snapshots": [s.snapshot for s in ctx.snapshots],
        "sources": ctx.sources,
        "tests": [t.test for t in ctx.tests],
        "unit_tests": ctx.unit_tests,
    }

    resource_map: dict[str, list[Any]] = {
        "catalog_nodes": ctx.catalog_nodes,
        "catalog_sources": ctx.catalog_sources,
        "exposures": ctx.exposures,
        "macros": ctx.macros,
        "models": ctx.models,
        "run_results": ctx.run_results,
        "seeds": ctx.seeds,
        "semantic_models": ctx.semantic_models,
        "snapshots": ctx.snapshots,
        "sources": ctx.sources,
        "tests": ctx.tests,
        "unit_tests": ctx.unit_tests,
    }

    # Pre-compute unique_id -> meta lookup for catalog_node skip_checks
    meta_by_unique_id: dict[str, Any] = {}
    for resource_key in ["models", "seeds", "snapshots"]:
        for resource in resource_map.get(resource_key, []):
            inner_attr = resource_key.rstrip("s")  # "models" -> "model"
            node = getattr(resource, inner_attr, None)
            if node is not None and hasattr(node, "unique_id"):
                try:
                    meta_by_unique_id[node.unique_id] = node.config.meta
                except AttributeError:
                    meta_by_unique_id[node.unique_id] = getattr(node, "meta", {})

    list_of_check_configs = []
    for check_category in ctx.check_categories:
        list_of_check_configs.extend(getattr(ctx.bouncer_config, check_category))

    checks_to_run: list[CheckToRun] = []
    for check in sorted(list_of_check_configs, key=operator.attrgetter("index")):
        cls = check.__class__
        if cls not in _CLASS_ITERATE_CACHE:
            _CLASS_ITERATE_CACHE[cls] = _VALID_ITERATE_OVER_VALUES.intersection(
                frozenset(cls.__annotations__.keys()),
            )
        iterate_over_value = _CLASS_ITERATE_CACHE[cls]
        if len(iterate_over_value) == 1:
            iterate_value = next(iter(iterate_over_value))
            for i in resource_map[f"{iterate_value}s"]:
                check_i = check.model_copy(deep=False)
                d = _get_resource_meta(i, iterate_value, meta_by_unique_id)
                meta_config = get_nested_value(
                    d,
                    ["dbt-bouncer", "skip_checks"],
                    [],
                )
                if _should_run_check(check_i, i, iterate_over_value, meta_config):
                    check_run_id = _build_check_run_id(check_i, i, iterate_value)
                    check_i._inject_context(
                        parsed_data, resource=i, iterate_over_value=iterate_value
                    )

                    checks_to_run.append(
                        {
                            "check": check_i,
                            "check_run_id": check_run_id,
                            "severity": check_i.severity,
                        },
                    )
        elif len(iterate_over_value) > 1:
            raise RuntimeError(
                f"Check {check.name} has multiple iterate_over_value values: {iterate_over_value}",
            )
        else:
            check_run_id = f"{check.name}:{check.index}"
            check._inject_context(parsed_data)
            checks_to_run.append(
                {
                    "check": check,
                    "check_run_id": check_run_id,
                    "severity": check.severity,
                },
            )

    del (
        ctx.models,
        ctx.run_results,
        ctx.seeds,
        ctx.semantic_models,
        ctx.snapshots,
        ctx.tests,
    )

    logging.info(f"Assembled {len(checks_to_run)} checks, running...")

    def _execute_check(check: CheckToRun) -> CheckToRun:
        """Execute a single check and return the result.

        Returns:
            dict[str, Any]: The check dict with outcome and optional failure_message set.

        """
        logging.debug(f"Running {check['check_run_id']}...")
        try:
            check["check"].execute()
            check["outcome"] = "success"
        except Exception as e:
            if isinstance(e, DbtBouncerFailedCheckError):
                failure_message = e.message
            else:
                failure_message_full = list(
                    traceback.TracebackException.from_exception(e).format(),
                )
                failure_message = failure_message_full[-1].strip()

            if check["check"].description:
                failure_message = f"{check['check'].description} - {failure_message}"

            logging.debug(
                f"Check {check['check_run_id']} failed: {' '.join(failure_message)}"
            )
            check["outcome"] = "failed"
            check["failure_message"] = failure_message

            # If a check encountered an issue, change severity to warn
            if not isinstance(e, DbtBouncerFailedCheckError):
                check["severity"] = "warn"
                check["failure_message"] = (
                    f"`dbt-bouncer` encountered an error ({failure_message}), run with `-v` to see more details or report an issue at https://github.com/godatadriven/dbt-bouncer/issues."
                )
        return check

    def _execute_batch(batch: list[CheckToRun]) -> int:
        """Execute all checks in a batch sequentially and return the count.

        Returns:
            int: Number of checks executed.

        """
        for check in batch:
            _execute_check(check)
        return len(batch)

    # Group checks by class to reduce ThreadPoolExecutor scheduling overhead.
    # Checks of the same class run sequentially within a batch; different
    # classes run in parallel across threads.
    # Cap batch size to avoid submitting one giant task for large check sets.
    batches: dict[str, list[CheckToRun]] = defaultdict(list)
    for check in checks_to_run:
        batches[check["check"].__class__.__name__].append(check)

    batches_to_run: list[list[CheckToRun]] = []
    for batch in batches.values():
        for i in range(0, max(len(batch), 1), _MAX_BATCH_SIZE):
            batches_to_run.append(batch[i : i + _MAX_BATCH_SIZE])

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
                executor.submit(_execute_batch, batch): batch
                for batch in batches_to_run
            }
            for future in as_completed(futures):
                batch_size = future.result()
                with progress_lock:
                    progress.update(task, advance=batch_size)

    results = [
        {
            "check_run_id": c["check_run_id"],
            "failure_message": c.get("failure_message"),
            "outcome": c["outcome"],
            "severity": c["severity"],
        }
        for c in checks_to_run
    ]
    num_checks_error = 0
    num_checks_warn = 0
    num_checks_success = 0
    for r in results:
        if r["outcome"] == "failed":
            if r["severity"] == "error":
                num_checks_error += 1
            else:
                num_checks_warn += 1
        else:
            num_checks_success += 1

    if num_checks_error > 0 or num_checks_warn > 0:
        logger = logging.error if num_checks_error > 0 else logging.warning
        logger(
            f"`dbt-bouncer` {'failed' if num_checks_error > 0 else 'has warnings'}. Please see below for more details or run `dbt-bouncer` with the `-v` flag."
            + (
                ""
                if num_checks_error < 25 or ctx.show_all_failures
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
            if r["outcome"] == "failed"
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
            failed_checks if ctx.show_all_failures else failed_checks[:25]
        )

        for check in checks_to_display:
            # Determine color based on severity string
            sev = str(check.get("severity", "")).lower()
            sev_color = "red" if "error" in sev else "yellow"

            table.add_row(
                str(check.get("check_run_id", "")),
                f"[bold {sev_color}]{sev.upper()}[/bold {sev_color}]",
                str(check.get("failure_message", "")),
            )

        console.print(table)

        if ctx.create_pr_comment_file:
            create_github_comment_file(
                failed_checks=[
                    [str(f["check_run_id"]), str(f.get("failure_message", ""))]
                    for f in failed_checks
                ],
                show_all_failures=ctx.show_all_failures,
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
        [r for r in results if r["outcome"] == "failed"]
        if ctx.output_only_failures
        else results
    )

    if ctx.output_file is not None:
        coverage_file = Path().cwd() / ctx.output_file
        logging.info(f"Saving coverage file to `{coverage_file}`.")
        coverage_file.write_bytes(_format_results(results_to_save, ctx.output_format))

    return 1 if num_checks_error != 0 else 0, results
