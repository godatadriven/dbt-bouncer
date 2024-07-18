import inspect
from typing import Dict, List

import pytest


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
    def __init__(self, model, *args, **kwargs):
        self.model: Dict[str, str] = model
        super().__init__(*args, **kwargs)


class GenerateTestsPlugin:
    """
    For fixtures that are lists (e.g. `models`) this plugin generates a test for each item in the list.
    Using alternaticve approaches like parametrize or fixture_params do not work, generating tests using
    `pytest_pycollect_makeitem` is one way to get this to work.
    """

    def __init__(self, models):
        self.models = models

    def pytest_pycollect_makeitem(self, collector, name, obj):
        items = []
        if (inspect.isfunction(obj) or inspect.ismethod(obj)) and (name.startswith("test_")):
            fixture_info = pytest.Function.from_parent(
                collector, name=name, callobj=obj
            )._fixtureinfo
            for model in self.models:
                item = MyFunctionItem.from_parent(
                    parent=collector,
                    name=name,
                    fixtureinfo=fixture_info,
                    model=model,
                )
                items.append(item)

        return items


def runner(
    models: List[Dict[str, str]],
    sources: List[Dict[str, str]],
    tests: List[Dict[str, str]],
) -> None:
    """
    Run pytest using fixtures from artifacts.
    """

    # Create a fixture plugin that can be used to inject the manifest into the tests
    fixtures = FixturePlugin()
    for att in ["models", "sources", "tests"]:
        setattr(fixtures, att + "_", locals()[att])

    # Run the tests, if one fails then pytest will raise an exception
    pytest.main(
        ["dbt_bouncer/tests"],
        plugins=[fixtures, GenerateTestsPlugin(models)],
    )
