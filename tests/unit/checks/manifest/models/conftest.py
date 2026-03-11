from contextlib import nullcontext as does_not_raise  # noqa: F401
from types import SimpleNamespace

import pytest

from dbt_bouncer.artifact_parsers.parser import wrap_dict


@pytest.fixture
def model(request):
    default_model = {
        "alias": "model_1",
        "checksum": {"name": "sha256", "checksum": ""},
        "columns": {
            "col_1": {
                "index": 1,
                "name": "col_1",
                "type": "INTEGER",
            },
        },
        "fqn": ["package_name", "model_1"],
        "language": "sql",
        "name": "model_1",
        "original_file_path": "model_1.sql",
        "package_name": "package_name",
        "path": "staging/finance/model_1.sql",
        "resource_type": "model",
        "schema": "main",
        "unique_id": "model.package_name.model_1",
    }
    return wrap_dict({**default_model, **getattr(request, "param", {})})


@pytest.fixture
def models(request):
    default_model = {
        "alias": "model_1",
        "checksum": {"name": "sha256", "checksum": ""},
        "columns": {
            "col_1": {
                "index": 1,
                "name": "col_1",
                "type": "INTEGER",
            },
        },
        "fqn": ["package_name", "model_1"],
        "name": "model_1",
        "original_file_path": "model_1.sql",
        "package_name": "package_name",
        "path": "staging/finance/model_1.sql",
        "resource_type": "model",
        "schema": "main",
        "unique_id": "model.package_name.model_1",
    }
    params = getattr(request, "param", [])
    return [wrap_dict({**default_model, **p}) for p in params]


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
    params = getattr(request, "param", [])
    return [wrap_dict({**default_test, **p}) for p in params]


@pytest.fixture
def exposures(request):
    default_exposure = {
        "depends_on": {"nodes": ["model.package_name.model_1"]},
        "fqn": ["package_name", "marts", "finance", "exposure_1"],
        "name": "exposure_1",
        "original_file_path": "models/marts/finance/_exposures.yml",
        "owner": {
            "email": "anna.anderson@example.com",
            "name": "Anna Anderson",
        },
        "package_name": "package_name",
        "path": "marts/finance/_exposures.yml",
        "resource_type": "exposure",
        "type": "dashboard",
        "unique_id": "exposure.package_name.exposure_1",
    }
    params = getattr(request, "param", [])
    return [wrap_dict({**default_exposure, **p}) for p in params]


@pytest.fixture
def unit_tests(request):
    default_unit_test = {
        "depends_on": {
            "nodes": [
                "model.package_name.model_1",
            ],
        },
        "expect": {"format": "dict", "rows": [{"id": 1}]},
        "fqn": [
            "package_name",
            "staging",
            "crm",
            "model_1",
            "unit_test_1",
        ],
        "given": [{"input": "ref(input_1)", "format": "csv"}],
        "model": "model_1",
        "name": "unit_test_1",
        "original_file_path": "models/staging/crm/_crm__source.yml",
        "resource_type": "unit_test",
        "package_name": "package_name",
        "path": "staging/crm/_crm__source.yml",
        "unique_id": "unit_test.package_name.model_1.unit_test_1",
    }
    params = getattr(request, "param", [])
    return [wrap_dict({**default_unit_test, **p}) for p in params]


@pytest.fixture
def manifest_obj(request):
    default_manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "1.11.0a1",
            "project_name": "dbt_bouncer_test_project",
            "adapter_type": "postgres",
        },
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

    params = getattr(request, "param", {})
    if params == "manifest_obj":
        params = {}

    if "manifest" in params:
        manifest_data = {**default_manifest, **params["manifest"]}
        return SimpleNamespace(manifest=wrap_dict(manifest_data))

    manifest_data = {**default_manifest, **params}
    return SimpleNamespace(manifest=wrap_dict(manifest_data))
