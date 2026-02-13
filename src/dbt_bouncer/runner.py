"""Assemble and run all checks."""

import logging
import operator
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any

import orjson
from progress.bar import Bar
from tabulate import tabulate

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import (
    create_github_comment_file,
    get_nested_value,
    resource_in_path,
)

if TYPE_CHECKING:
    from dbt_bouncer.context import BouncerContext


def _should_run_check(
    check: Any,
    resource: Any,
    iterate_over_value: set[str],
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
        "run_results": [r.run_result for r in ctx.run_results],
        "seeds": [s.seed for s in ctx.seeds],
        "semantic_models": [s.semantic_model for s in ctx.semantic_models],
        "snapshots": [s.snapshot for s in ctx.snapshots],
        "sources": ctx.sources,
        "tests": [t.test for t in ctx.tests],
        "unit_tests": ctx.unit_tests,
    }

    resource_map: dict[str, Any] = {
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

    checks_to_run = []
    for check in sorted(list_of_check_configs, key=operator.attrgetter("index")):
        valid_iterate_over_values = {
            "catalog_node",
            "catalog_source",
            "exposure",
            "macro",
            "model",
            "run_result",
            "seed",
            "semantic_model",
            "snapshot",
            "source",
            "test",
            "unit_test",
        }
        iterate_over_value = valid_iterate_over_values.intersection(
            set(check.__class__.__annotations__.keys()),
        )
        if len(iterate_over_value) == 1:
            iterate_value = next(iter(iterate_over_value))
            for i in resource_map[f"{iterate_value}s"]:
                check_i = check.model_copy(deep=True)
                if iterate_value in [
                    "model",
                    "seed",
                    "semantic_model",
                    "snapshot",
                    "source",
                ]:
                    try:
                        d = getattr(i, iterate_value).config.meta
                    except AttributeError:
                        d = getattr(i, iterate_value).meta
                elif iterate_value == "catalog_node":
                    d = meta_by_unique_id.get(getattr(i, "unique_id", ""), {})
                elif iterate_value == "run_result":
                    d = {}
                elif iterate_value in ["macro"]:
                    d = i.meta
                elif iterate_value == "test":
                    d = getattr(getattr(i, "test", i), "meta", {}) or {}
                else:
                    try:
                        d = i.config.meta
                    except AttributeError:
                        d = i.meta
                meta_config = get_nested_value(
                    d,
                    ["dbt-bouncer", "skip_checks"],
                    [],
                )
                if _should_run_check(check_i, i, iterate_over_value, meta_config):
                    check_run_id = (
                        f"{check_i.name}:{check_i.index}:{i.unique_id.split('.')[-1]}"
                        if iterate_value in ["exposure", "macro", "test", "unit_test"]
                        else f"{check_i.name}:{check_i.index}:{'_'.join(getattr(i, iterate_value).unique_id.split('.')[2:])}"
                    )
                    setattr(check_i, iterate_value, getattr(i, iterate_value, i))

                    for x in (
                        parsed_data.keys() & check_i.__class__.__annotations__.keys()
                    ):
                        setattr(check_i, x, parsed_data[x])

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
            for x in parsed_data.keys() & check.__class__.__annotations__.keys():
                setattr(check, x, parsed_data[x])
            checks_to_run.append(
                {
                    "check": check,
                    "check_run_id": check_run_id,
                    "severity": check.severity,
                },
            )

    logging.info(f"Assembled {len(checks_to_run)} checks, running...")

    def _execute_check(check: dict[str, Any]) -> dict[str, Any]:
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

    bar = Bar("Running checks...", max=len(checks_to_run))
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(_execute_check, check): check for check in checks_to_run
        }
        for future in as_completed(futures):
            future.result()
            bar.next()
    bar.finish()

    results = [
        {
            "check_run_id": c["check_run_id"],
            "failure_message": c.get("failure_message"),
            "outcome": c["outcome"],
            "severity": c["severity"],
        }
        for c in checks_to_run
    ]
    num_checks_error = len(
        [c for c in results if c["outcome"] == "failed" and c["severity"] == "error"]
    )
    num_checks_warn = len(
        [c for c in results if c["outcome"] == "failed" and c["severity"] == "warn"]
    )
    num_checks_success = len([c for c in results if c["outcome"] == "success"])

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
        logger(
            ("Failed checks:\n" if num_checks_error > 0 else "Warning checks:\n")
            + tabulate(
                failed_checks if ctx.show_all_failures else failed_checks[:25],
                headers={
                    "check_run_id": "Check name",
                    "severity": "Severity",
                    "failure_message": "Failure message",
                },
                tablefmt="github",
            ),
        )

        if ctx.create_pr_comment_file:
            create_github_comment_file(
                failed_checks=[
                    [f["check_run_id"], f["failure_message"]] for f in failed_checks
                ],
                show_all_failures=ctx.show_all_failures,
            )

    logging.info(
        f"Done. SUCCESS={num_checks_success} WARN={num_checks_warn} ERROR={num_checks_error}",
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


def _format_results(results: list[dict[str, Any]], output_format: str) -> bytes:
    """Serialise check results to the requested format.

    Args:
        results: List of check result dicts.
        output_format: One of "csv", "json", "junit", "sarif", or "tap".

    Returns:
        bytes: Serialised results.

    Raises:
        ValueError: If output_format is not recognised.

    """
    match output_format:
        case "csv":
            return _format_csv(results)
        case "json":
            return orjson.dumps(results)
        case "junit":
            return _format_junit(results)
        case "sarif":
            return _format_sarif(results)
        case "tap":
            return _format_tap(results)
        case _:
            msg = f"Unknown output format: {output_format}"
            raise ValueError(msg)


def _format_junit(results: list[dict[str, Any]]) -> bytes:
    """Serialise check results to JUnit XML format.

    Each check result becomes a TestCase. Failed checks are marked with a
    <failure> element; warn-severity failures use type="warn".

    Args:
        results: List of check result dicts.

    Returns:
        bytes: JUnit XML document.

    """
    import io

    from junitparser import Failure, JUnitXml, TestCase, TestSuite

    test_cases = []
    for result in results:
        tc = TestCase(
            name=result["check_run_id"],
            classname="dbt-bouncer",
        )
        if result["outcome"] == "failed":
            tc.result = [
                Failure(
                    message=result.get("failure_message") or "",
                    type_=result.get("severity", "error"),
                )
            ]
        test_cases.append(tc)

    suite = TestSuite("dbt-bouncer")
    for tc in test_cases:
        suite.add_testcase(tc)

    xml = JUnitXml()
    xml.add_testsuite(suite)
    buf = io.BytesIO()
    xml.write(buf, pretty=True)
    return buf.getvalue()


def _format_csv(results: list[dict[str, Any]]) -> bytes:
    """Serialise check results to CSV format.

    Args:
        results: List of check result dicts.

    Returns:
        bytes: CSV document.

    """
    import csv
    import io

    buf = io.StringIO()
    fieldnames = ["check_run_id", "outcome", "severity", "failure_message"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)
    return buf.getvalue().encode()


def _format_sarif(results: list[dict[str, Any]]) -> bytes:
    """Serialise check results to SARIF 2.1.0 format.

    Args:
        results: List of check result dicts.

    Returns:
        bytes: SARIF JSON document.

    """
    sarif_results = []
    for r in results:
        level = "warning" if r.get("severity") == "warn" else "error"
        if r["outcome"] == "failed":
            sarif_results.append(
                {
                    "ruleId": r["check_run_id"],
                    "level": level,
                    "message": {"text": r.get("failure_message") or "Check failed"},
                }
            )
        else:
            sarif_results.append(
                {
                    "ruleId": r["check_run_id"],
                    "level": "none",
                    "message": {"text": "Check passed"},
                }
            )

    sarif = {
        "$schema": "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/main/sarif-2.1/schema/sarif-schema-2.1.0.json",
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "dbt-bouncer",
                        "informationUri": "https://github.com/godatadriven/dbt-bouncer",
                    },
                },
                "results": sarif_results,
            },
        ],
    }
    return orjson.dumps(sarif, option=orjson.OPT_INDENT_2)


def _format_tap(results: list[dict[str, Any]]) -> bytes:
    """Serialise check results to TAP (Test Anything Protocol) format.

    Args:
        results: List of check result dicts.

    Returns:
        bytes: TAP document.

    """
    lines = ["TAP version 13", f"1..{len(results)}"]
    for i, r in enumerate(results, 1):
        status = "ok" if r["outcome"] != "failed" else "not ok"
        lines.append(f"{status} {i} - {r['check_run_id']}")
        if r["outcome"] == "failed" and r.get("failure_message"):
            for msg_line in r["failure_message"].splitlines():
                lines.append(f"  # {msg_line}")
    return "\n".join(lines).encode()
