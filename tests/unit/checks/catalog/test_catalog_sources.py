import warnings
from contextlib import nullcontext as does_not_raise

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.catalog.catalog_v1 import Nodes as CatalogNodes

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import Sources
from dbt_bouncer.artifact_parsers.parsers_manifest import (
    DbtBouncerSourceBase,  # noqa: F401
)
from dbt_bouncer.checks.catalog.check_catalog_sources import (
    CheckSourceColumnsAreAllDocumented,
)
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

CheckSourceColumnsAreAllDocumented.model_rebuild()


@pytest.fixture
def catalog_source(request):
    default_catalog_source = {
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
        "unique_id": "source.package_name.source_1.table_1",
    }
    return CatalogNodes(**{**default_catalog_source, **getattr(request, "param", {})})


@pytest.fixture
def sources(request):
    default_source = {
        "columns": {
            "col_1": {
                "name": "col_1",
            },
            "col_2": {
                "name": "col_2",
            },
        },
        "fqn": ["package_name", "source_1", "table_1"],
        "identifier": "table_1",
        "loader": "csv",
        "name": "table_1",
        "original_file_path": "path/to/source_1.yml",
        "package_name": "package_name",
        "path": "path/to/source_1.yml",
        "resource_type": "source",
        "schema": "main",
        "source_description": "",
        "source_name": "source_1",
        "unique_id": "source.package_name.source_1.table_1",
    }
    return [Sources(**{**default_source, **getattr(request, "param", {})})]


_TEST_DATA_FOR_CHECK_SOURCE_COLUMNS_ARE_ALL_DOCUMENTED = [
    pytest.param(
        {},
        {},
        does_not_raise(),
        id="all_documented",
    ),
    pytest.param(
        {},
        {
            "columns": {
                "col_1": {
                    "name": "col_1",
                },
            },
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_documentation",
    ),
]


@pytest.mark.parametrize(
    ("catalog_source", "sources", "expectation"),
    _TEST_DATA_FOR_CHECK_SOURCE_COLUMNS_ARE_ALL_DOCUMENTED,
    indirect=["catalog_source", "sources"],
)
def test_check_source_columns_are_all_documented(catalog_source, sources, expectation):
    with expectation:
        CheckSourceColumnsAreAllDocumented(
            catalog_source=catalog_source,
            name="check_source_columns_are_all_documented",
            sources=sources,
        ).execute()
