# TODO Remove after this program no longer support Python 3.8.*
from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import Any, List, Optional, Union

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Exposures, Macros, UnitTests

import logging
from functools import wraps

import click
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
from dbt_bouncer.runner_plugins import (
    FixturePlugin,
    GenerateTestsPlugin,
    ResultsCollector,
)
from dbt_bouncer.utils import create_github_comment_file, bouncer_check_v2, object_in_path
import importlib
from pydantic._internal._model_construction import ModelMetaclass


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
    checks_dir: Optional[Union[None, Path]] = Path(__file__).parent / "checks",
) -> tuple[int, List[Any]]:
    """
    Run dbt-bouncer checks.
    """

    # logging.warning(f"{bouncer_config=}")

    # Dynamically import all Check classes
    check_files = [f for f in (Path(__file__).parent / "checks").glob("*/*.py") if f.is_file()]
    for check_file in check_files:
        imported_check_file = importlib.import_module(
            ".".join([x.replace(".py", "") for x in check_file.parts[-4:]])
        )
        for obj in dir(imported_check_file):
            if callable(getattr(imported_check_file, obj)) and obj.startswith("check_"):
                locals()[obj] = getattr(imported_check_file, obj)
                # logging.info(f"{obj=}")

    # logging.warning(sorted(locals().keys()))
    # logging.warning(sorted(globals().keys()))

    parsed_data = {
        "catalog_nodes": catalog_nodes,
        "catalog_nodes": catalog_sources,
        "exposures": exposures,
        "macros": macros,
        "manifest_obj": manifest_obj,
        "models": [m.model for m in models],
        "run_results": [r.run_result for r in run_results],
        "sources": sources,
        "tests": [t.test for t in tests],
        "unit_tests": unit_tests,
    }

    def resource_in_path(check, resource) -> bool:
        return object_in_path(check.include, resource.original_file_path) and not (
            check.exclude is not None
            and object_in_path(check.exclude, resource.original_file_path)
        )

    for k, v in sorted(bouncer_config.items()):
        for check in v:
            # logging.warning(f"{check=}")
            check_run_info = {**check.model_dump(), **parsed_data}
            
            valid_iterate_over_values = {"catalog_node", "catalog_source", "exposure", "macro", "model", "run_result", "source", "unit_test"}
            iterate_over_value = valid_iterate_over_values.intersection(set(locals()[check.name].__annotations__.keys()))
            assert len(iterate_over_value) == 1, f"Check {check.name} must have one and only one of {valid_iterate_over_values}"
            # logging.warning(f"{iterate_over_value=}")
            iterate_value = list(iterate_over_value)[0]
            if iterate_value == "catalog_node":
                x = "catalog_node"
            elif iterate_value == "catalog_source":
                x = "catalog_source"
            elif iterate_value == "exposure":
                x = "exposure"
            elif iterate_value == "macro":
                x = "macro"
            elif iterate_value == "model":
                x = "model"
            elif iterate_value == "run_result":
                x = "run_result"
            elif iterate_value == "source":
                x = "source"
            elif iterate_value == "unit_test":
                x = "unit_test"
            else:
                raise RuntimeError
            
            # logging.warning(f"{x=}")
            # logging.warning(f"{len(run_results)=}")
            # logging.warning(len(locals()[f"{x}s"]))
            
            for i in locals()[f"{x}s"]:
                # logging.warning("here0")
                # logging.warning(f"{check=}")
                # logging.warning(f"{i=}")
                # logging.warning(resource_in_path(check, i))
                
                if resource_in_path(check, i):
                    # logging.warning("here1")
                    check_run_id = (
                        f"{check.name}:{check.index}:{i.unique_id.split('.')[2]}"
                    )
                    logging.info(f"Running {check_run_id}...")
                    locals()[check.name](
                        **{**check_run_info, **{x: getattr(i, x, i)}}
                    )
        

    return 0, None

    results = [report._to_json() for report in collector.reports]
    num_failed_checks = len([report for report in collector.reports if report.outcome == "failed"])

    if num_failed_checks > 0:
        logging.error("`dbt-bouncer` failed. Please check the logs above for more details.")
        failed_checks = [
            [
                r["nodeid"],
                r["longrepr"]["chain"][0][1]["message"].split("\n")[0],
            ]
            for r in results
            if r["outcome"] == "failed"
        ]
        logging.debug(f"{failed_checks=}")
        logging.error(
            "Failed checks:\n"
            + tabulate(
                failed_checks,
                headers=["Check name", "Failure message"],
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

    return 1 if run_checks != 0 else 0, results
