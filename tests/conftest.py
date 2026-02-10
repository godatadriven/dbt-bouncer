import json
import warnings
from pathlib import Path

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
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
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

    from dbt_bouncer.config_file_parser import DbtBouncerConfAllCategories

    DbtBouncerConfAllCategories.model_rebuild(_types_namespace=types_namespace)


@pytest.fixture(scope="session")
def manifest_obj():
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        parse_manifest,
    )

    manifest_json_path = Path("dbt_project") / "target/manifest.json"
    with Path.open(manifest_json_path, "r") as fp:
        manifest_obj = parse_manifest(manifest=json.load(fp))
    return DbtBouncerManifest(**{"manifest": manifest_obj})
