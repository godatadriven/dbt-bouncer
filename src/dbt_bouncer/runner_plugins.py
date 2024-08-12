import inspect
from typing import Dict

import pytest

from dbt_bouncer.logger import logger
from dbt_bouncer.utils import object_in_path


class FixturePlugin(object):
    def __init__(
        self,
        catalog_nodes,
        catalog_sources,
        exposures,
        macros,
        manifest_obj,
        models,
        run_results,
        sources,
        tests,
    ):
        self.catalog_nodes_ = catalog_nodes
        self.catalog_sources_ = catalog_sources
        self.exposures_ = exposures
        self.macros_ = macros
        self.manifest_obj_ = manifest_obj
        self.models_ = models
        self.run_results_ = run_results
        self.sources_ = sources
        self.tests_ = tests

    @pytest.fixture(scope="session")
    def catalog_nodes(self):
        return self.catalog_nodes_

    @pytest.fixture(scope="session")
    def catalog_sources(self):
        return self.catalog_sources_

    @pytest.fixture(scope="session")
    def exposures(self):
        return self.exposures_

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
    def run_results(self):
        return self.run_results_

    @pytest.fixture(scope="session")
    def sources(self):
        return self.sources_

    @pytest.fixture(scope="session")
    def tests(self):
        return self.tests_


# Inspiration: https://github.com/pytest-dev/pytest-xdist/discussions/957#discussioncomment-7335007
class MyFunctionItem(pytest.Function):
    def __init__(
        self,
        check_config,
        catalog_node=None,
        catalog_source=None,
        exposure=None,
        macro=None,
        model=None,
        run_result=None,
        source=None,
        *args,
        **kwargs,
    ):
        self.check_config: Dict[str, str] = check_config
        self.catalog_node: Dict[str, str] | None = catalog_node
        self.catalog_source: Dict[str, str] | None = catalog_source
        self.exposure: Dict[str, str] | None = exposure
        self.macro: Dict[str, str] | None = macro
        self.model: Dict[str, str] | None = model
        self.run_result: Dict[str, str] | None = run_result
        self.source: Dict[str, str] | None = source
        super().__init__(*args, **kwargs)


class GenerateTestsPlugin:
    """
    For fixtures that are lists (e.g. `models`) this plugin generates a check for each item in the list.
    Using alternative approaches like parametrize or fixture_params do not work, generating checks using
    `pytest_pycollect_makeitem` is one way to get this to work.
    """

    def __init__(
        self,
        bouncer_config,
        catalog_nodes,
        catalog_sources,
        exposures,
        macros,
        models,
        run_results,
        sources,
    ):
        self.bouncer_config = bouncer_config
        self.catalog_nodes = catalog_nodes
        self.catalog_sources = catalog_sources
        self.exposures = exposures
        self.macros = macros
        self.models = models
        self.run_results = run_results
        self.sources = sources

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
                    ).keywords._markers.keys()  # type: ignore[attr-defined]
                    if (
                        len(
                            set(
                                [
                                    "iterate_over_catalog_nodes",
                                    "iterate_over_catalog_sources",
                                    "iterate_over_exposures",
                                    "iterate_over_models",
                                    "iterate_over_macros",
                                    "iterate_over_run_results",
                                    "iterate_over_sources",
                                ]
                            ).intersection(markers)
                        )
                        > 0
                    ):
                        key = [m for m in markers if m.startswith("iterate_over_")][0].replace(
                            "iterate_over_", ""
                        )
                        logger.debug(f"{key=}")
                        for x in self.__getattribute__(key):
                            if key == "catalog_nodes":
                                catalog_node = x
                                catalog_source = exposure = macro = model = run_result = source = (
                                    None
                                )
                            elif key == "catalog_sources":
                                catalog_source = x
                                catalog_node = exposure = macro = model = run_result = source = (
                                    None
                                )
                            elif key == "exposures":
                                exposure = x
                                catalog_node = catalog_source = macro = model = run_result = (
                                    source
                                ) = None
                            elif key == "macros":
                                macro = x
                                catalog_node = catalog_source = exposure = model = run_result = (
                                    source
                                ) = None
                            elif key == "models":
                                model = x
                                catalog_node = catalog_source = exposure = macro = run_result = (
                                    source
                                ) = None
                            elif key == "run_results":
                                run_result = x
                                catalog_node = catalog_source = exposure = macro = model = (
                                    source
                                ) = None
                            elif key == "sources":
                                source = x
                                catalog_node = catalog_source = exposure = macro = model = (
                                    run_result
                                ) = None

                            if object_in_path(check_config.get("include"), x["path"]):
                                item = MyFunctionItem.from_parent(
                                    parent=collector,
                                    name=name,
                                    fixtureinfo=fixture_info,
                                    check_config=check_config,
                                    catalog_node=catalog_node,
                                    catalog_source=catalog_source,
                                    exposure=exposure,
                                    macro=macro,
                                    model=model,
                                    run_result=run_result,
                                    source=source,
                                )
                                item._nodeid = f"{name}::{x['unique_id'].split('.')[-1]}_{check_config['index']}"

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
