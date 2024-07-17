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
    pytest.main(["dbt_bouncer/tests"], plugins=[fixtures])
