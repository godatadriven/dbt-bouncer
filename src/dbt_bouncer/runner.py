"""Assemble and run all checks."""

# TODO Remove after this program no longer support Python 3.8.*
from __future__ import annotations

import json
import logging
import operator
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Union

from progress.bar import Bar
from tabulate import tabulate

from dbt_bouncer.utils import (
    create_github_comment_file,
    get_check_objects,
    resource_in_path,
)

if TYPE_CHECKING:
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (
        Exposures,
        Macros,
        UnitTests,
    )

    from dbt_bouncer.config_file_validator import DbtBouncerConf
    from dbt_bouncer.parsers import (
        DbtBouncerCatalogNode,
        DbtBouncerManifest,
        DbtBouncerModel,
        DbtBouncerRunResult,
        DbtBouncerSemanticModel,
        DbtBouncerSource,
        DbtBouncerTest,
    )


def runner(
    bouncer_config: DbtBouncerConf,
    catalog_nodes: List[DbtBouncerCatalogNode],
    catalog_sources: List[DbtBouncerCatalogNode],
    check_categories: List[str],
    create_pr_comment_file: bool,
    exposures: List[Exposures],
    macros: List[Macros],
    manifest_obj: DbtBouncerManifest,
    models: List[DbtBouncerModel],
    output_file: Union[None, Path],
    run_results: List[DbtBouncerRunResult],
    semantic_models: List[DbtBouncerSemanticModel],
    sources: List[DbtBouncerSource],
    tests: List[DbtBouncerTest],
    unit_tests: List[UnitTests],
) -> tuple[int, List[Any]]:
    """Run dbt-bouncer checks.

    Returns:
        tuple[int, List[Any]]: A tuple containing the exit code and a list of failed checks.

    Raises:
        RuntimeError: If more than one "iterate_over" argument is found.

    """
    for i in get_check_objects()["functions"]:
        locals()[i.__name__] = getattr(i, i.__name__)

    parsed_data = {
        "catalog_nodes": catalog_nodes,
        "catalog_sources": catalog_sources,
        "exposures": exposures,
        "macros": macros,
        "manifest_obj": manifest_obj,
        "models": [m.model for m in models],
        "run_results": [r.run_result for r in run_results],
        "semantic_models": [s.semantic_model for s in semantic_models],
        "sources": sources,
        "tests": [t.test for t in tests],
        "unit_tests": unit_tests,
    }

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
            "semantic_model",
            "source",
            "unit_test",
        }
        iterate_over_value = valid_iterate_over_values.intersection(
            set(locals()[check.name].__annotations__.keys()),
        )

        if len(iterate_over_value) == 1:
            iterate_value = next(iter(iterate_over_value))

            for i in locals()[f"{iterate_value}s"]:
                if resource_in_path(check, i):
                    check_run_id = (
                        f"{check.name}:{check.index}:{i.unique_id.split('.')[2]}"
                    )
                    checks_to_run.append(
                        {
                            **{
                                "check_run_id": check_run_id,
                            },
                            **check.model_dump(),
                            **{iterate_value: getattr(i, iterate_value, i)},
                        },
                    )
        elif len(iterate_over_value) > 1:
            raise RuntimeError(
                f"Check {check.name} has multiple iterate_over_value values: {iterate_over_value}",
            )
        else:
            check_run_id = f"{check.name}:{check.index}"
            checks_to_run.append(
                {
                    **{
                        "check_run_id": check_run_id,
                    },
                    **check.model_dump(),
                },
            )

    logging.info(f"Assembled {len(checks_to_run)} checks, running...")

    bar = Bar("Running checks...", max=len(checks_to_run))
    for check in checks_to_run:
        logging.debug(f"Running {check['check_run_id']}...")
        try:
            locals()[check["name"]](**{**check, **parsed_data})
            check["outcome"] = "success"
        except AssertionError as e:
            failure_message = list(
                traceback.TracebackException.from_exception(e).format(),
            )[-1].strip()
            logging.debug(f"Check {check['check_run_id']} failed: {failure_message}")
            check["outcome"] = "failed"
            check["failure_message"] = failure_message
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
    logging.info(
        f"Done. SUCCESS={num_checks_success} WARN={num_checks_warn} ERROR={num_checks_error}",
    )

    if num_checks_error > 0 or num_checks_warn > 0:
        logger = logging.error if num_checks_error > 0 else logging.warning
        logger(
            f"`dbt-bouncer` {'failed' if num_checks_error > 0 else 'has warnings'}. Please see below for more details or run `dbt-bouncer` with the `-v` flag...",
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
                failed_checks,
                headers={
                    "check_run_id": "Check name",
                    "severity": "Severity",
                    "failure_message": "Failure message",
                },
                tablefmt="github",
            ),
        )

        if create_pr_comment_file:
            create_github_comment_file(failed_checks=failed_checks)

    if output_file is not None:
        coverage_file = Path().cwd() / output_file
        logging.info(f"Saving coverage file to `{coverage_file}`.")
        with Path.open(coverage_file, "w") as f:
            json.dump(
                results,
                f,
            )

    return 1 if num_checks_error != 0 else 0, results
