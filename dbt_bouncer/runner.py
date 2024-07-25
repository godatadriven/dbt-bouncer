import sys
from pathlib import Path
from typing import Dict, List

import pytest
from tabulate import tabulate

from dbt_bouncer.logger import logger
from dbt_bouncer.runner_plugins import (
    FixturePlugin,
    GenerateTestsPlugin,
    ResultsCollector,
)


def runner(
    bouncer_config: Dict[str, List[Dict[str, str]]],
    macros: List[Dict[str, str]],
    manifest_obj: Dict[str, str],
    models: List[Dict[str, str]],
    sources: List[Dict[str, str]],
    tests: List[Dict[str, str]],
) -> None:
    """
    Run pytest using fixtures from artifacts.
    """

    # Create a fixture plugin that can be used to inject the manifest into the checks
    fixtures = FixturePlugin()
    for att in ["macros", "manifest_obj", "models", "sources", "tests"]:
        setattr(fixtures, att + "_", locals()[att])

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
                bouncer_config=bouncer_config, macros=macros, models=models, sources=sources
            ),
        ],
    )
    if run_checks.value != 0:  # type: ignore[attr-defined]
        logger.error(
            "Failed checks:\n"
            + tabulate(
                [
                    [
                        report.nodeid,
                        report._to_json()["longrepr"]["chain"][0][1]["message"].split("\n")[0],
                    ]
                    for report in collector.reports
                    if report.outcome == "failed"
                ],
                headers=["Check name", "Failure message"],
                tablefmt="github",
            )
        )
        sys.exit(1)
