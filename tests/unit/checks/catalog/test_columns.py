from contextlib import nullcontext as does_not_raise

import pytest
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes4, Nodes6

from dbt_bouncer.checks.catalog.check_columns import (
    check_column_has_specified_test,
    check_column_name_complies_to_column_type,
    check_columns_are_all_documented,
    check_columns_are_documented_in_public_models,
)
from dbt_bouncer.parsers import DbtBouncerCatalogNode


@pytest.mark.parametrize(
    "catalog_node, check_config, tests, expectation",
    [
        (
            DbtBouncerCatalogNode(
                **{
                    "node": {
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
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
                    },
                    "path": "marts/finance/_models.yml",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            {"column_name_pattern": ".*_1$", "test_name": "unique"},
            [
                Nodes6(
                    **{
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
                ),
            ],
            does_not_raise(),
        ),
        (
            DbtBouncerCatalogNode(
                **{
                    "node": {
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
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
                    },
                    "path": "marts/finance/_models.yml",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            {"column_name_pattern": ".*_1$", "test_name": "unique"},
            [
                Nodes6(
                    **{
                        "alias": "not_null_model_1_not_null",
                        "attached_node": "model.package_name.model_1",
                        "checksum": {"name": "none", "checksum": ""},
                        "column_name": "col_1",
                        "fqn": ["package_name", "marts", "finance", "not_null_model_1_not_null"],
                        "name": "not_null_model_1_not_null",
                        "original_file_path": "models/marts/finance/_finance__models.yml",
                        "package_name": "package_name",
                        "path": "not_null_model_1_not_null.sql",
                        "resource_type": "test",
                        "schema": "main",
                        "test_metadata": {
                            "name": "not_null",
                        },
                        "unique_id": "test.package_name.not_null_model_1_not_null.cf6c17daed",
                    }
                )
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_column_has_specified_test(catalog_node, check_config, tests, expectation):
    with expectation:
        check_column_has_specified_test(
            catalog_node=catalog_node, check_config=check_config, tests=tests, request=None
        )


@pytest.mark.parametrize(
    "catalog_node, models, expectation",
    [
        (
            DbtBouncerCatalogNode(
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
                        "unique_id": "model.package_name.model_1",
                    },
                    "path": "marts/finance/_models.yml",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            [
                Nodes4(
                    **{
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
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
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    }
                )
            ],
            does_not_raise(),
        ),
        (
            DbtBouncerCatalogNode(
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
                        "unique_id": "model.package_name.model_2",
                    },
                    "path": "marts/finance/_models.yml",
                    "unique_id": "model.package_name.model_2",
                }
            ),
            [
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "model_2.sql",
                        "package_name": "package_name",
                        "path": "model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                )
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_columns_are_all_documented(catalog_node, models, expectation):
    with expectation:
        check_columns_are_all_documented(catalog_node=catalog_node, models=models, request=None)


@pytest.mark.parametrize(
    "catalog_node, models, expectation",
    [
        (
            DbtBouncerCatalogNode(
                **{
                    "node": {
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
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
                    },
                    "path": "marts/finance/_models.yml",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            [
                Nodes4(
                    **{
                        "access": "public",
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "description": "This is a description",
                                "index": 1,
                                "name": "col_1",
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
                )
            ],
            does_not_raise(),
        ),
        (
            DbtBouncerCatalogNode(
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
                        "unique_id": "model.package_name.model_1",
                    },
                    "path": "marts/finance/_models.yml",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            [
                Nodes4(
                    **{
                        "access": "public",
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
                                "description": "",
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
                )
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_columns_are_documented_in_public_models(catalog_node, models, expectation):
    with expectation:
        check_columns_are_documented_in_public_models(
            catalog_node=catalog_node, models=models, request=None
        )


@pytest.mark.parametrize(
    "catalog_node, check_config, expectation",
    [
        (
            DbtBouncerCatalogNode(
                **{
                    "node": {
                        "columns": {
                            "col_1_date": {
                                "index": 1,
                                "name": "col_1_date",
                                "type": "DATE",
                            },
                        },
                        "metadata": {
                            "name": "table_1",
                            "schema": "main",
                            "type": "VIEW",
                        },
                        "stats": {},
                        "unique_id": "model.package_name.model_1",
                    },
                    "path": "marts/finance/_models.yml",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            {"column_name_pattern": ".*_date$", "types": ["DATE"]},
            does_not_raise(),
        ),
        (
            DbtBouncerCatalogNode(
                **{
                    "node": {
                        "columns": {
                            "col_1_date": {
                                "index": 1,
                                "name": "col_1_date",
                                "type": "DATE",
                            },
                            "col_2_date": {
                                "index": 2,
                                "name": "col_2_date",
                                "type": "DATE",
                            },
                            "col_3": {
                                "index": 3,
                                "name": "col_3",
                                "type": "VARCHAR",
                            },
                        },
                        "metadata": {
                            "name": "table_1",
                            "schema": "main",
                            "type": "VIEW",
                        },
                        "stats": {},
                        "unique_id": "model.package_name.model_1",
                    },
                    "path": "marts/finance/_models.yml",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            {"column_name_pattern": ".*_date$", "types": ["DATE"]},
            does_not_raise(),
        ),
        (
            DbtBouncerCatalogNode(
                **{
                    "node": {
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "DATE",
                            },
                        },
                        "metadata": {
                            "name": "table_1",
                            "schema": "main",
                            "type": "VIEW",
                        },
                        "stats": {},
                        "unique_id": "model.package_name.model_1",
                    },
                    "path": "marts/finance/_models.yml",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            {"column_name_pattern": ".*_date$", "types": ["DATE"]},
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_column_name_complies_to_column_type(catalog_node, check_config, expectation):
    with expectation:
        check_column_name_complies_to_column_type(
            catalog_node=catalog_node, check_config=check_config, request=None
        )
