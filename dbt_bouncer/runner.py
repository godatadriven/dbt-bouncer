import inspect
from pathlib import Path
from typing import Dict, List

import pytest

from dbt_bouncer.logger import logger


class FixturePlugin(object):
    def __init__(self):
        self.models_ = None
        self.sources_ = None
        self.tests_ = None

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
    def __init__(self, check_config, model=None, *args, **kwargs):
        self.check_config: Dict[str, str] = check_config
        self.model: Dict[str, str] | None = model
        super().__init__(*args, **kwargs)


class GenerateTestsPlugin:
    """
    For fixtures that are lists (e.g. `models`) this plugin generates a check for each item in the list.
    Using alternative approaches like parametrize or fixture_params do not work, generating checks using
    `pytest_pycollect_makeitem` is one way to get this to work.
    """

    def __init__(self, bouncer_config, models):
        self.bouncer_config = bouncer_config
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
                            item = MyFunctionItem.from_parent(
                                parent=collector,
                                name=name,
                                fixtureinfo=fixture_info,
                                model=model,
                                check_config=check_config,
                            )
                            item._nodeid = f"{name}::{model['name']}_{check_config['index']}"
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


def runner(
    bouncer_config: Dict[str, List[Dict[str, str]]],
    models: List[Dict[str, str]],
    sources: List[Dict[str, str]],
    tests: List[Dict[str, str]],
) -> None:
    """
    Run pytest using fixtures from artifacts.
    """

    # Create a fixture plugin that can be used to inject the manifest into the checks
    fixtures = FixturePlugin()
    for att in ["models", "sources", "tests"]:
        setattr(fixtures, att + "_", locals()[att])

    # Run the checks, if one fails then pytest will raise an exception
    pytest.main(
        [
            "-c",
            (Path(__file__).parent / "checks").__str__(),
            (Path(__file__).parent / "checks").__str__(),
            "-s",
            "-vv",
        ],
        plugins=[fixtures, GenerateTestsPlugin(bouncer_config=bouncer_config, models=models)],
    )
