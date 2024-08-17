from contextlib import nullcontext as does_not_raise

import pytest
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Sources

from dbt_bouncer.checks.catalog.check_catalog_sources import (
    check_source_columns_are_all_documented,
)
from dbt_bouncer.parsers import DbtBouncerCatalog


@pytest.mark.parametrize(
    "catalog_source, sources, expectation",
    [
        (
            DbtBouncerCatalog(
                **{
                    "node": {
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
                    },
                    "path": "path/to/source_1.yml",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            [
                Sources(
                    **{
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
                )
            ],
            does_not_raise(),
        ),
        (
            DbtBouncerCatalog(
                **{
                    "node": {
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                            "col_2": {
                                "index": 1,
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
                    },
                    "path": "path/to/source_1.yml",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            [
                Sources(
                    **{
                        "columns": {
                            "col_1": {
                                "name": "col_1",
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
                )
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_columns_are_all_documented(catalog_source, sources, expectation):
    with expectation:
        check_source_columns_are_all_documented(
            catalog_source=catalog_source, sources=sources, request=None
        )
