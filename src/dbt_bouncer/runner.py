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
    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
        UnitTests,
    )
    from dbt_bouncer.artifact_parsers.parsers_common import (
        DbtBouncerCatalogNode,
        DbtBouncerManifest,
        DbtBouncerModel,
        DbtBouncerRunResult,
        DbtBouncerSeed,
        DbtBouncerSemanticModel,
        DbtBouncerSnapshot,
        DbtBouncerSource,
        DbtBouncerTest,
    )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerExposureBase,
        DbtBouncerMacroBase,
    )
    from dbt_bouncer.config_file_parser import DbtBouncerConfBase


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
    bouncer_config: "DbtBouncerConfBase",
    catalog_nodes: list["DbtBouncerCatalogNode"],
    catalog_sources: list["DbtBouncerCatalogNode"],
    check_categories: list[str],
    create_pr_comment_file: bool,
    exposures: list["DbtBouncerExposureBase"],
    macros: list["DbtBouncerMacroBase"],
    manifest_obj: "DbtBouncerManifest",
    models: list["DbtBouncerModel"],
    output_file: Path | None,
    run_results: list["DbtBouncerRunResult"],
    seeds: list["DbtBouncerSeed"],
    semantic_models: list["DbtBouncerSemanticModel"],
    output_only_failures: bool,
    show_all_failures: bool,
    snapshots: list["DbtBouncerSnapshot"],
    sources: list["DbtBouncerSource"],
    tests: list["DbtBouncerTest"],
    unit_tests: list["UnitTests"],
) -> tuple[int, list[Any]]:
    """Run dbt-bouncer checks.

    Returns:
        tuple[int, list[Any]]: A tuple containing the exit code and a list of failed checks.

    Raises:
        RuntimeError: If more than one "iterate_over" argument is found.

    """
    parsed_data = {
        "catalog_nodes": catalog_nodes,
        "catalog_sources": catalog_sources,
        "exposures": exposures,
        "macros": macros,
        "manifest_obj": manifest_obj,
        "models": [m.model for m in models],
        "run_results": [r.run_result for r in run_results],
        "seeds": [s.seed for s in seeds],
        "semantic_models": [s.semantic_model for s in semantic_models],
        "snapshots": [s.snapshot for s in snapshots],
        "sources": sources,
        "tests": [t.test for t in tests],
        "unit_tests": unit_tests,
    }

    resource_map: dict[str, Any] = {
        "catalog_nodes": catalog_nodes,
        "catalog_sources": catalog_sources,
        "exposures": exposures,
        "macros": macros,
        "models": models,
        "run_results": run_results,
        "seeds": seeds,
        "semantic_models": semantic_models,
        "snapshots": snapshots,
        "sources": sources,
        "unit_tests": unit_tests,
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
    for check_category in check_categories:
        list_of_check_configs.extend(getattr(bouncer_config, check_category))

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
                        if iterate_value in ["exposure", "macro", "unit_test"]
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
                if num_checks_error < 25 or show_all_failures
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
                failed_checks if show_all_failures else failed_checks[:25],
                headers={
                    "check_run_id": "Check name",
                    "severity": "Severity",
                    "failure_message": "Failure message",
                },
                tablefmt="github",
            ),
        )

        if create_pr_comment_file:
            create_github_comment_file(
                failed_checks=[
                    [f["check_run_id"], f["failure_message"]] for f in failed_checks
                ],
                show_all_failures=show_all_failures,
            )

    logging.info(
        f"Done. SUCCESS={num_checks_success} WARN={num_checks_warn} ERROR={num_checks_error}",
    )

    if output_file is not None:
        coverage_file = Path().cwd() / output_file
        logging.info(f"Saving coverage file to `{coverage_file}`.")

        if output_only_failures:
            results_to_save = [r for r in results if r["outcome"] == "failed"]
        else:
            results_to_save = results

        coverage_file.write_bytes(orjson.dumps(results_to_save))

    return 1 if num_checks_error != 0 else 0, results
