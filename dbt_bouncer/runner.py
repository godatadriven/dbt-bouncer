import inspect
import re
import sys
from pathlib import Path
from typing import Dict, List

import pytest
from tabulate import tabulate

from dbt_bouncer.logger import logger


class FixturePlugin(object):
    def __init__(self):
        self.macros_ = None
        self.manifest_obj_ = None
        self.models_ = None
        self.sources_ = None
        self.tests_ = None

    @pytest.fixture(scope="session")
    def macros(self):
        return self.macros_

    @pytest.fixture(scope="session")
    def manifest_obj(self):
        return self.manifest_obj_

    @pytest.fixture(scope="session")
    def models(self):
        return self.models_

    @pytest.fixture(scope="session")
    def sources(self):
        return self.sources_

    @pytest.fixture(scope="session")
    def tests(self):
        return self.tests_


# Inspiration: https://github.com/pytest-dev/pytest-xdist/discussions/957#discussioncomment-7335007
class MyFunctionItem(pytest.Function):
    def __init__(self, check_config, macro=None, model=None, *args, **kwargs):
        self.check_config: Dict[str, str] = check_config
        self.macro: Dict[str, str] | None = macro
        self.model: Dict[str, str] | None = model
        super().__init__(*args, **kwargs)


class GenerateTestsPlugin:
    """
    For fixtures that are lists (e.g. `models`) this plugin generates a check for each item in the list.
    Using alternative approaches like parametrize or fixture_params do not work, generating checks using
    `pytest_pycollect_makeitem` is one way to get this to work.
    """

    def __init__(self, bouncer_config, macros, models):
        self.bouncer_config = bouncer_config
        self.macros = macros
        self.models = models

    def pytest_pycollect_makeitem(self, collector, name, obj):
        items = []
        if name in self.bouncer_config.keys():
            for check_config in self.bouncer_config[name]:
                logger.debug(f"{check_config=}")

                if (inspect.isfunction(obj) or inspect.ismethod(obj)) and (
                    name.startswith("check_")
                ):
                    fixture_info = pytest.Function.from_parent(
                        collector, name=name, callobj=obj
                    )._fixtureinfo

                    markers = pytest.Function.from_parent(
                        collector, name=name
                    ).keywords._markers.keys()
                    if "iterate_over_models" in markers:
                        for model in self.models:
                            if (
                                check_config.get("include") is not None
                                and re.compile(check_config["include"].strip()).match(
                                    model["path"]
                                )
                                is None
                            ):
                                pass
                            else:
                                item = MyFunctionItem.from_parent(
                                    parent=collector,
                                    name=name,
                                    fixtureinfo=fixture_info,
                                    model=model,
                                    check_config=check_config,
                                )
                                item._nodeid = f"{name}::{model['name']}_{check_config['index']}"
                                items.append(item)
                    elif "iterate_over_macros" in markers:
                        for macro in self.macros:
                            if (
                                check_config.get("include") is not None
                                and re.compile(check_config["include"].strip()).match(
                                    model["path"]
                                )
                                is None
                            ):
                                pass
                            else:
                                item = MyFunctionItem.from_parent(
                                    parent=collector,
                                    name=name,
                                    fixtureinfo=fixture_info,
                                    macro=macro,
                                    check_config=check_config,
                                )
                                item._nodeid = f"{name}::{macro['name']}_{check_config['index']}"
                                items.append(item)
                    else:
                        item = MyFunctionItem.from_parent(
                            parent=collector,
                            name=name,
                            fixtureinfo=fixture_info,
                            check_config=check_config,
                        )
                        item._nodeid = f"{name}_{check_config['index']}"
                        items.append(item)
        else:
            logger.debug(f"Skipping check {name} because it is not in the checks list.")

        return items


class ResultsCollector:
    def __init__(self):
        self.exitcode = 0
        self.reports = []

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self):
        outcome = yield
        report = outcome.get_result()
        if report.when == "call":
            self.reports.append(report)


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
            "-s",
        ],
        plugins=[
            collector,
            fixtures,
            GenerateTestsPlugin(bouncer_config=bouncer_config, macros=macros, models=models),
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
