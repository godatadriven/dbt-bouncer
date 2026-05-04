from pathlib import Path
from types import SimpleNamespace

import orjson
import pytest

from dbt_bouncer.artifact_parsers.parser import wrap_dict


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "not_in_parallel: Unit test that cannot be run in parallel."
    )
    config.addinivalue_line(
        "markers", "not_in_parallel2: Unit test that cannot be run in parallel."
    )


@pytest.fixture(autouse=True, scope="session")
def _rebuild_all_check_models():
    """Rebuild all Check* Pydantic models to resolve forward references.

    Since we no longer use Pydantic artifact types, this only needs to
    rebuild check classes and the bouncer conf/context with Any stubs.
    """
    from dbt_bouncer.check_framework.exceptions import NestedDict

    types_namespace = {
        "NestedDict": NestedDict,
    }

    from dbt_bouncer.utils import get_check_objects

    for check_cls in get_check_objects():
        check_cls.model_rebuild(_types_namespace=types_namespace)

    from dbt_bouncer.configuration_file.parser import create_bouncer_conf_class

    dbt_bouncer_conf_all_categories = create_bouncer_conf_class()
    dbt_bouncer_conf_all_categories.model_rebuild(_types_namespace=types_namespace)

    from dbt_bouncer.context import BouncerContext

    BouncerContext.model_rebuild(_types_namespace=types_namespace)


@pytest.fixture(autouse=True)
def _clear_module_caches():
    """Clear module-level memoization caches between tests.

    Clearing these between tests prevents state leaking across test boundaries
    when class annotations are patched, models are rebuilt with different
    namespaces, or Python reuses object addresses after garbage collection.
    """
    from dbt_bouncer import runner
    from dbt_bouncer.checks.manifest import check_macros

    check_macros._USED_MACROS_CACHE.clear()
    runner._CLASS_ITERATE_CACHE.clear()
    yield
    check_macros._USED_MACROS_CACHE.clear()
    runner._CLASS_ITERATE_CACHE.clear()


@pytest.fixture(scope="session")
def manifest_obj():
    manifest_json_path = Path("dbt_project") / "target/manifest.json"
    manifest_data = orjson.loads(manifest_json_path.read_bytes())
    return SimpleNamespace(manifest=wrap_dict(manifest_data))
