import sys
from pathlib import Path
from typing import Dict, List

import pytest
from tabulate import tabulate

from dbt_bouncer.github import send_github_comment_failed_checks
from dbt_bouncer.logger import logger
from dbt_bouncer.runner_plugins import (
    FixturePlugin,
    GenerateTestsPlugin,
    ResultsCollector,
)


def runner(
    bouncer_config: Dict[str, List[Dict[str, str]]],
    catalog_nodes: List[Dict[str, str]],
    catalog_sources: List[Dict[str, str]],
    exposures: List[Dict[str, str]],
    macros: List[Dict[str, str]],
    manifest_obj: Dict[str, str],
    models: List[Dict[str, str]],
    run_results: List[Dict[str, str]],
    send_pr_comment: bool,
    sources: List[Dict[str, str]],
    tests: List[Dict[str, str]],
) -> None:
    """
    Run pytest using fixtures from artifacts.
    """

    # Create a fixture plugin that can be used to inject the manifest into the checks
    fixtures = FixturePlugin(
        catalog_nodes,
        catalog_sources,
        exposures,
        macros,
        manifest_obj,
        models,
        run_results,
        sources,
        tests,
    )

    # Run the checks, if one fails then pytest will raise an exception
    collector = ResultsCollector()
    run_checks = pytest.main(
        [
            "-c",
            (Path(__file__).parent / "checks").__str__(),
            (Path(__file__).parent / "checks").__str__(),
        ],
        plugins=[
            collector,
            fixtures,
            GenerateTestsPlugin(
                bouncer_config=bouncer_config,
                catalog_nodes=catalog_nodes,
                catalog_sources=catalog_sources,
                exposures=exposures,
                macros=macros,
                models=models,
                run_results=run_results,
                sources=sources,
            ),
        ],
    )
    if run_checks.value != 0:  # type: ignore[attr-defined]
        failed_checks = [
            [
                report.nodeid,
                report._to_json()["longrepr"]["chain"][0][1]["message"].split("\n")[0],
            ]
            for report in collector.reports
            if report.outcome == "failed"
        ]
        logger.debug(f"{failed_checks=}")
        logger.error(
            "Failed checks:\n"
            + tabulate(
                failed_checks,
                headers=["Check name", "Failure message"],
                tablefmt="github",
            )
        )
        if send_pr_comment:
            send_github_comment_failed_checks(failed_checks=failed_checks)

        logger.error("`dbt-bouncer` failed. Please check the logs above for more details.")
        sys.exit(1)
