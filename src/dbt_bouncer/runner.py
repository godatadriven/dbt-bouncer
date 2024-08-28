# TODO Remove after this program no longer support Python 3.8.*
from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import Any, List, Union

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Exposures, Macros, UnitTests

import importlib
import logging
import traceback

from tabulate import tabulate

from dbt_bouncer.conf_validator import DbtBouncerConf
from dbt_bouncer.parsers import (
    DbtBouncerCatalogNode,
    DbtBouncerManifest,
    DbtBouncerModel,
    DbtBouncerRunResult,
    DbtBouncerSource,
    DbtBouncerTest,
)
from dbt_bouncer.utils import create_github_comment_file, resource_in_path


def runner(
    bouncer_config: DbtBouncerConf,
    catalog_nodes: List[DbtBouncerCatalogNode],
    catalog_sources: List[DbtBouncerCatalogNode],
    create_pr_comment_file: bool,
    exposures: List[Exposures],
    macros: List[Macros],
    manifest_obj: DbtBouncerManifest,
    models: List[DbtBouncerModel],
    output_file: Union[None, Path],
    run_results: List[DbtBouncerRunResult],
    sources: List[DbtBouncerSource],
    tests: List[DbtBouncerTest],
    unit_tests: List[UnitTests],
) -> tuple[int, List[Any]]:
    """
    Run dbt-bouncer checks.
    """

    # Dynamically import all Check classes
    check_files = [f for f in (Path(__file__).parent / "checks").glob("*/*.py") if f.is_file()]
    for check_file in check_files:
        imported_check_file = importlib.import_module(
            ".".join([x.replace(".py", "") for x in check_file.parts[-4:]])
        )
        for obj in dir(imported_check_file):
            if callable(getattr(imported_check_file, obj)) and obj.startswith("check_"):
                locals()[obj] = getattr(imported_check_file, obj)

    parsed_data = {
        "catalog_nodes": catalog_nodes,
        "catalog_sources": catalog_sources,
        "exposures": exposures,
        "macros": macros,
        "manifest_obj": manifest_obj,
        "models": [m.model for m in models],
        "run_results": [r.run_result for r in run_results],
        "sources": sources,
        "tests": [t.test for t in tests],
        "unit_tests": unit_tests,
    }

    checks_to_run = []
    for _, v in sorted(bouncer_config.items()):
        for check in v:
            valid_iterate_over_values = {
                "catalog_node",
                "catalog_source",
                "exposure",
                "macro",
                "model",
                "run_result",
                "source",
                "unit_test",
            }
            iterate_over_value = valid_iterate_over_values.intersection(
                set(locals()[check.name].__annotations__.keys())
            )

            if len(iterate_over_value) == 1:
                iterate_value = list(iterate_over_value)[0]

                for i in locals()[f"{iterate_value}s"]:
                    if resource_in_path(check, i):
                        check_run_id = f"{check.name}:{check.index}:{i.unique_id.split('.')[2]}"
                        checks_to_run.append(
                            {
                                **{
                                    "check_run_id": check_run_id,
                                },
                                **check.model_dump(),
                                **{iterate_value: getattr(i, iterate_value, i)},
                            }
                        )
            elif len(iterate_over_value) > 1:
                raise RuntimeError(
                    f"Check {check.name} has multiple iterate_over_value values: {iterate_over_value}"
                )
            else:
                check_run_id = f"{check.name}:{check.index}"
                checks_to_run.append(
                    {
                        **{
                            "check_run_id": check_run_id,
                        },
                        **check.model_dump(),
                    }
                )

    logging.info(f"Assembled {len(checks_to_run)} checks, running...")

    for check in checks_to_run:
        logging.debug(f"Running {check['check_run_id']}...")
        try:
            locals()[check["name"]](**{**check, **parsed_data})
            check["outcome"] = "success"
        except AssertionError as e:
            failure_message = list(traceback.TracebackException.from_exception(e).format())[
                -1
            ].strip()
            logging.debug(f"Check {check['check_run_id']} failed: {failure_message}")
            check["outcome"] = "failed"
            check["failure_message"] = failure_message

    results = [
        {
            "check_run_id": c["check_run_id"],
            "failure_message": c.get("failure_message"),
            "outcome": c["outcome"],
        }
        for c in checks_to_run
    ]
    num_failed_checks = len([c for c in results if c["outcome"] == "failed"])
    num_succeeded_checks = len([c for c in results if c["outcome"] == "success"])
    logging.info(
        f"Summary: {num_succeeded_checks} checks passed, {num_failed_checks} checks failed."
    )

    if num_failed_checks > 0:
        logging.error("`dbt-bouncer` failed. Please check the logs above for more details.")
        failed_checks = [
            {"check_run_id": r["check_run_id"], "failure_message": r["failure_message"]}
            for r in results
            if r["outcome"] == "failed"
        ]
        logging.debug(f"{failed_checks=}")
        logging.error(
            "Failed checks:\n"
            + tabulate(
                failed_checks,
                headers={"check_run_id": "Check name", "failure_message": "Failure message"},
                tablefmt="github",
            )
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

    return 1 if num_failed_checks != 0 else 0, results
