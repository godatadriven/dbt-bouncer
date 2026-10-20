import os
from pathlib import Path
from types import SimpleNamespace

import orjson
import pytest
import yaml
from hypothesis import HealthCheck, settings

from dbt_bouncer.artifact_parsers.parser import wrap_dict

_HYPOTHESIS_COMMON_SETTINGS = {
    "deadline": None,
    "suppress_health_check": [HealthCheck.too_slow],
}
settings.register_profile("ci", max_examples=100, **_HYPOTHESIS_COMMON_SETTINGS)
settings.register_profile("default", max_examples=50, **_HYPOTHESIS_COMMON_SETTINGS)
settings.register_profile("quick", max_examples=15, **_HYPOTHESIS_COMMON_SETTINGS)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))


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
    from dbt_bouncer import runner, utils
    from dbt_bouncer.checks.manifest import check_macros

    def _clear():
        check_macros._USED_MACROS_CACHE.clear()
        runner._CLASS_ITERATE_CACHE.clear()
        # Entry points are scanned once per process; tests that monkeypatch the
        # installed set need the scan re-run.
        utils._check_entry_points.cache_clear()
        utils._check_entry_point_names.cache_clear()
        # The session-scoped model-rebuild fixture populates this before any
        # test runs, so a test that monkeypatches import behaviour would
        # otherwise be served a stale registry -- and, worse, could leave a
        # deliberately broken one behind for whatever runs next.
        utils.get_check_objects.cache_clear()

    _clear()
    yield
    _clear()


@pytest.fixture(scope="session")
def manifest_obj():
    manifest_json_path = Path("dbt_project") / "target/manifest.json"
    manifest_data = orjson.loads(manifest_json_path.read_bytes())
    return SimpleNamespace(manifest=wrap_dict(manifest_data))


@pytest.fixture(scope="session")
def dbt_112_target_dir() -> Path:
    """Return the committed dbt 1.12 fixture artifacts as an absolute path.

    Returns:
        Path: The `tests/fixtures/dbt_112/target` directory.

    """
    return (Path(__file__).parent / "fixtures" / "dbt_112" / "target").resolve()


@pytest.fixture
def failing_config_file(dbt_112_target_dir: Path, tmp_path: Path) -> Path:
    """Write a config whose single check fails on every model.

    No model name starts with "zzz_", so `check_model_names` fails on all of them.

    Returns:
        Path: The config file path.

    """
    config_file = tmp_path / "custom-bouncer.yml"
    config_file.write_text(
        yaml.dump(
            {
                "dbt_artifacts_dir": str(dbt_112_target_dir),
                "manifest_checks": [
                    {"name": "check_model_names", "model_name_pattern": "^zzz_"}
                ],
            }
        )
    )
    return config_file
