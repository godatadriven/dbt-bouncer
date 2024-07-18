import importlib
from pathlib import Path


def pytest_collection_modifyitems(session, config, items):
    """
    Pytest runs all functions that start with "test_". This is an issue as out functions in
    `./dbt_bouncer/tests` also have this prefix and therefore pytest tries to test those.
    This hook is used to dynamically retrieve the names of these functions and remove them
    from the list of functions that pytest will run.
    """

    dbt_bouncer_tests: list[str] = []
    for f in Path("./dbt_bouncer/tests").glob("**/*"):
        if f.is_file() and f.name.startswith("test_") and f.name.endswith(".py"):
            test_file = importlib.import_module(f"dbt_bouncer.tests.{f.stem}")
            dbt_bouncer_tests.extend(
                i for i in dir(test_file) if not i.startswith("_") and i != "logger"
            )

    items[:] = [item for item in items if item.name not in dbt_bouncer_tests]
