import warnings
from contextlib import nullcontext as does_not_raise  # noqa: F401

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.catalog.catalog_v1 import Nodes as CatalogNodes

    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
        ManifestLatest,
        Metadata,
    )

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import Nodes4, Nodes6
from dbt_bouncer.artifact_parsers.parsers_manifest import DbtBouncerManifest


@pytest.fixture
def catalog_node(request):
    default_catalog_node = {
        "columns": {
            "col_1": {
                "index": 1,
                "name": "col_1",
                "type": "INTEGER",
            },
            "col_2": {
                "index": 2,
                "name": "col_2",
                "type": "INTEGER",
            },
        },
        "metadata": {
            "name": "table_1",
            "schema": "main",
            "type": "VIEW",
        },
        "stats": {},
        "unique_id": "model.package_name.model_1",
    }
    return CatalogNodes(**{**default_catalog_node, **getattr(request, "param", {})})


@pytest.fixture
def models(request):
    default_model = {
        "alias": "model_1",
        "checksum": {"name": "sha256", "checksum": ""},
        "columns": {
            "col_1": {
                "description": "This is a description",
                "index": 1,
                "name": "col_1",
                "type": "INTEGER",
            },
            "col_2": {
                "description": "This is a description",
                "index": 2,
                "name": "col_2",
                "type": "INTEGER",
            },
        },
        "fqn": ["package_name", "model_1"],
        "name": "model_1",
        "original_file_path": "model_1.sql",
        "package_name": "package_name",
        "path": "model_1.sql",
        "resource_type": "model",
        "schema": "main",
        "unique_id": "model.package_name.model_1",
    }
    return [Nodes4(**{**default_model, **getattr(request, "param", {})})]


@pytest.fixture
def tests(request):
    default_test = {
        "alias": "not_null_model_1_unique",
        "attached_node": "model.package_name.model_1",
        "checksum": {"name": "none", "checksum": ""},
        "column_name": "col_1",
        "fqn": [
            "package_name",
            "marts",
            "finance",
            "not_null_model_1_unique",
        ],
        "name": "not_null_model_1_unique",
        "original_file_path": "models/marts/finance/_finance__models.yml",
        "package_name": "package_name",
        "path": "not_null_model_1_unique.sql",
        "resource_type": "test",
        "schema": "main",
        "test_metadata": {
            "name": "unique",
        },
        "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
    }
    return [Nodes6(**{**default_test, **getattr(request, "param", {})})]


@pytest.fixture
def manifest_obj(request):
    default_manifest = {
        "metadata": Metadata(
            dbt_schema_version="https://schemas.getdbt.com/dbt/manifest/v12.json",
            dbt_version="1.11.0a1",
            generated_at=None,
            invocation_id=None,
            invocation_started_at=None,
            env=None,
            project_name="dbt_bouncer_test_project",
            project_id=None,
            user_id=None,
            send_anonymous_usage_stats=None,
            adapter_type="postgres",
            quoting=None,
            run_started_at=None,
        ),
        "nodes": {},
        "sources": {},
        "macros": {},
        "docs": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "disabled": {},
        "parent_map": {},
        "child_map": {},
        "group_map": {},
        "saved_queries": {},
        "semantic_models": {},
        "unit_tests": {},
        "functions": None,
    }

    # Handle DbtBouncerManifest wrapping
    params = getattr(request, "param", {})
    if params == "manifest_obj":  # Default marker used in existing tests
        params = {}

    # If params provides ManifestLatest, use it, else create one
    if "manifest" in params:
        return DbtBouncerManifest(**params)

    adapter_type = params.pop("adapter_type", None)
    if adapter_type:
        default_manifest["metadata"] = default_manifest["metadata"].model_copy(
            update={"adapter_type": adapter_type}
        )

    manifest_data = {**default_manifest, **params}
    return DbtBouncerManifest(manifest=ManifestLatest(**manifest_data))
