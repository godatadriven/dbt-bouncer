import inspect
from typing import Dict

import pytest

from dbt_bouncer.logger import logger
from dbt_bouncer.utils import object_in_path


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
    def __init__(self, check_config, macro=None, model=None, source=None, *args, **kwargs):
        self.check_config: Dict[str, str] = check_config
        self.macro: Dict[str, str] | None = macro
        self.model: Dict[str, str] | None = model
        self.source: Dict[str, str] | None = source
        super().__init__(*args, **kwargs)


class GenerateTestsPlugin:
    """
    For fixtures that are lists (e.g. `models`) this plugin generates a check for each item in the list.
    Using alternative approaches like parametrize or fixture_params do not work, generating checks using
    `pytest_pycollect_makeitem` is one way to get this to work.
    """

    def __init__(self, bouncer_config, macros, models, sources):
        self.bouncer_config = bouncer_config
        self.macros = macros
        self.models = models
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
                                    "iterate_over_models",
                                    "iterate_over_macros",
                                    "iterate_over_sources",
                                ]
                            ).intersection(markers)
                        )
                        > 0
                    ):
                        key = [m for m in markers if m.startswith("iterate_over_")][0].split("_")[
                            -1
                        ]
                        logger.debug(f"{key=}")
                        for x in self.__getattribute__(key):
                            if key == "macros":
                                key, macro, model, source = "macros", x, None, None
                            elif key == "models":
                                key, macro, model, source = "models", None, x, None
                            elif key == "sources":
                                key, macro, model, source = "source", None, None, x

                            if object_in_path(check_config.get("include"), x["path"]):
                                item = MyFunctionItem.from_parent(
                                    parent=collector,
                                    name=name,
                                    fixtureinfo=fixture_info,
                                    check_config=check_config,
                                    macro=macro,
                                    model=model,
                                    source=source,
                                )
                                item._nodeid = f"{name}::{x['name']}_{check_config['index']}"
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
