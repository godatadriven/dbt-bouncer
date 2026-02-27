from pathlib import Path

import orjson
import pytest


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

    This replaces the per-file model_rebuild() calls that were previously
    scattered across every test module.
    """
    from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
        Nodes as CatalogNodes,
    )

    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
        Macros,
        UnitTests,
    )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerExposureBase,
        DbtBouncerManifest,
        DbtBouncerModelBase,
        DbtBouncerSeedBase,
        DbtBouncerSemanticModelBase,
        DbtBouncerSnapshotBase,
        DbtBouncerSourceBase,
        DbtBouncerTestBase,
    )
    from dbt_bouncer.artifact_parsers.parsers_run_results import (
        DbtBouncerRunResultBase,
    )
    from dbt_bouncer.checks.common import NestedDict

    types_namespace = {
        "CatalogNodes": CatalogNodes,
        "DbtBouncerExposureBase": DbtBouncerExposureBase,
        "DbtBouncerManifest": DbtBouncerManifest,
        "DbtBouncerModelBase": DbtBouncerModelBase,
        "DbtBouncerRunResultBase": DbtBouncerRunResultBase,
        "DbtBouncerSeedBase": DbtBouncerSeedBase,
        "DbtBouncerSemanticModelBase": DbtBouncerSemanticModelBase,
        "DbtBouncerSnapshotBase": DbtBouncerSnapshotBase,
        "DbtBouncerSourceBase": DbtBouncerSourceBase,
        "DbtBouncerTestBase": DbtBouncerTestBase,
        "Macros": Macros,
        "NestedDict": NestedDict,
        "UnitTests": UnitTests,
    }

    from dbt_bouncer.utils import get_check_objects

    for check_cls in get_check_objects():
        check_cls.model_rebuild(_types_namespace=types_namespace)

    from dbt_bouncer.config_file_parser import create_bouncer_conf_class

    dbt_bouncer_conf_all_categories = create_bouncer_conf_class()
    dbt_bouncer_conf_all_categories.model_rebuild(_types_namespace=types_namespace)


@pytest.fixture(autouse=True)
def _clear_module_caches():
    """Clear module-level memoization caches between tests.

    Both _CLASS_ITERATE_CACHE (runner.py) and _ANNOTATION_KEYS_CACHE
    (check_base.py) are pure derivations of class structure. Clearing them
    between tests prevents state leaking across test boundaries when class
    annotations are patched or models are rebuilt with different namespaces.
    """
    from dbt_bouncer import runner
    from dbt_bouncer.check_base import _ANNOTATION_KEYS_CACHE

    runner._CLASS_ITERATE_CACHE.clear()
    _ANNOTATION_KEYS_CACHE.clear()
    yield
    runner._CLASS_ITERATE_CACHE.clear()
    _ANNOTATION_KEYS_CACHE.clear()


@pytest.fixture(scope="session")
def manifest_obj():
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        parse_manifest,
    )

    manifest_json_path = Path("dbt_project") / "target/manifest.json"
    manifest_obj = parse_manifest(
        manifest=orjson.loads(manifest_json_path.read_bytes())
    )
    return DbtBouncerManifest(**{"manifest": manifest_obj})
