import warnings
from contextlib import nullcontext as does_not_raise

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.catalog.catalog_v1 import Nodes as CatalogNodes

    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
        ManifestLatest,
        Metadata,
    )

from pydantic import ValidationError

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import Nodes4, Nodes6
from dbt_bouncer.artifact_parsers.parsers_manifest import (  # noqa: F401  # noqa: F401
    DbtBouncerManifest,
    DbtBouncerModelBase,
    DbtBouncerTest,
    DbtBouncerTestBase,
)
from dbt_bouncer.checks.catalog.check_columns import (
    CheckColumnDescriptionPopulated,
    CheckColumnHasSpecifiedTest,
    CheckColumnNameCompliesToColumnType,
    CheckColumnNames,
    CheckColumnsAreAllDocumented,
    CheckColumnsAreDocumentedInPublicModels,
)

CheckColumnDescriptionPopulated.model_rebuild()
CheckColumnNameCompliesToColumnType.model_rebuild()
CheckColumnNames.model_rebuild()
CheckColumnsAreAllDocumented.model_rebuild()
CheckColumnsAreDocumentedInPublicModels.model_rebuild()
CheckColumnHasSpecifiedTest.model_rebuild()


@pytest.mark.parametrize(
    ("catalog_node", "models", "expectation"),
    [
        (
            CatalogNodes(
                **{
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
            ),
            [
                Nodes4(
                    **{
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
                    },
                ),
            ],
            does_not_raise(),
        ),
        (
            CatalogNodes(
                **{
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
            ),
            [
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "description": "This is a description",
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
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "model_2.sql",
                        "package_name": "package_name",
                        "path": "model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    },
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_column_description_populated(catalog_node, models, expectation):
    with expectation:
        CheckColumnDescriptionPopulated(
            catalog_node=catalog_node,
            models=models,
            name="check_column_description_populated",
        ).execute()


@pytest.mark.parametrize(
    ("catalog_node", "column_name_pattern", "test_name", "tests", "expectation"),
    [
        (
            CatalogNodes(
                **{
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
            ),
            ".*_1$",
            "unique",
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
                    },
                ),
            ],
            does_not_raise(),
        ),
        (
            CatalogNodes(
                **{
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
            ),
            ".*_1$",
            "unique",
            [
                Nodes6(
                    **{
                        "alias": "not_null_model_1_not_null",
                        "attached_node": "model.package_name.model_1",
                        "checksum": {"name": "none", "checksum": ""},
                        "column_name": "col_1",
                        "fqn": [
                            "package_name",
                            "marts",
                            "finance",
                            "not_null_model_1_not_null",
                        ],
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
                    },
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_column_has_specified_test(
    catalog_node,
    column_name_pattern,
    test_name,
    tests,
    expectation,
):
    with expectation:
        CheckColumnHasSpecifiedTest(
            catalog_node=catalog_node,
            column_name_pattern=column_name_pattern,
            name="check_column_has_specified_test",
            test_name=test_name,
            tests=tests,
        ).execute()


@pytest.mark.parametrize(
    ("catalog_node", "manifest_obj", "models", "expectation"),
    [
        (
            CatalogNodes(
                **{
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
            ),
            "manifest_obj",
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
                    },
                ),
            ],
            does_not_raise(),
        ),
        (
            CatalogNodes(
                **{
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
            ),
            "manifest_obj",
            [
                Nodes4(
                    **{
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "COL_1": {
                                "index": 1,
                                "name": "COL_1",
                                "type": "INTEGER",
                            },
                            "COL_2": {
                                "index": 2,
                                "name": "COL_2",
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
                    },
                ),
            ],
            pytest.raises(AssertionError),
        ),
        (
            CatalogNodes(
                **{
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
            ),
            "manifest_obj",
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
                    },
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_columns_are_all_documented(
    catalog_node, manifest_obj, models, expectation
):
    with expectation:
        CheckColumnsAreAllDocumented(
            catalog_node=catalog_node,
            manifest_obj=manifest_obj,
            models=models,
            name="check_columns_are_all_documented",
        ).execute()


@pytest.mark.parametrize(
    ("catalog_node", "manifest_obj", "models", "expectation"),
    [
        (
            CatalogNodes(
                **{
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
            ),
            DbtBouncerManifest(
                **{
                    "manifest": ManifestLatest(
                        metadata=Metadata(
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
                            adapter_type="snowflake",
                            quoting=None,
                            run_started_at=None,
                        ),
                        nodes={},
                        sources={},
                        macros={},
                        docs={},
                        exposures={},
                        metrics={},
                        groups={},
                        selectors={},
                        disabled={},
                        parent_map={},
                        child_map={},
                        group_map={},
                        saved_queries={},
                        semantic_models={},
                        unit_tests={},
                        functions=None,
                    )
                }
            ),
            [
                Nodes4(
                    **{
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "COL_1": {
                                "index": 1,
                                "name": "COL_1",
                                "type": "INTEGER",
                            },
                            "COL_2": {
                                "index": 2,
                                "name": "COL_2",
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
                    },
                ),
            ],
            does_not_raise(),
        )
    ],
)
def test_check_columns_are_all_documented_snowflake(
    catalog_node, manifest_obj, models, expectation
):
    with expectation:
        CheckColumnsAreAllDocumented(
            catalog_node=catalog_node,
            manifest_obj=manifest_obj,
            models=models,
            name="check_columns_are_all_documented",
        ).execute()


@pytest.mark.parametrize(
    ("catalog_node", "models", "expectation"),
    [
        (
            CatalogNodes(
                **{
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
                    },
                ),
            ],
            does_not_raise(),
        ),
        (
            CatalogNodes(
                **{
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
                    },
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_columns_are_documented_in_public_models(
    catalog_node,
    models,
    expectation,
):
    with expectation:
        CheckColumnsAreDocumentedInPublicModels(
            catalog_node=catalog_node,
            models=models,
            name="check_columns_are_documented_in_public_models",
        ).execute()


@pytest.mark.parametrize(
    ("catalog_node", "column_name_pattern", "type_pattern", "types", "expectation"),
    [
        (
            CatalogNodes(
                **{
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
            ),
            ".*_date$",
            None,
            ["DATE"],
            does_not_raise(),
        ),
        (
            CatalogNodes(
                **{
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
            ),
            ".*_date$",
            None,
            ["DATE"],
            does_not_raise(),
        ),
        (
            CatalogNodes(
                **{
                    "columns": {
                        "col_1_date": {
                            "index": 1,
                            "name": "col_1_date",
                            "type": "DATE",
                        },
                        "col_2_date": {
                            "index": 2,
                            "name": "col_2_date",
                            "type": "STRUCT",
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
            ),
            ".*_date$",
            "^(?!STRUCT)",
            None,
            pytest.raises(AssertionError),
        ),
        (
            CatalogNodes(
                **{
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
            ),
            ".*_date$",
            None,
            ["DATE"],
            pytest.raises(AssertionError),
        ),
        (
            CatalogNodes(
                **{
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
            ),
            ".*_date$",
            None,
            None,
            pytest.raises(ValidationError),
        ),
        (
            CatalogNodes(
                **{
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
            ),
            ".*_date$",
            "^(?!STRUCT)",
            ["DATE"],
            pytest.raises(ValidationError),
        ),
    ],
)
def test_check_column_name_complies_to_column_type(
    catalog_node,
    column_name_pattern,
    type_pattern,
    types,
    expectation,
):
    with expectation:
        CheckColumnNameCompliesToColumnType(
            catalog_node=catalog_node,
            column_name_pattern=column_name_pattern,
            name="check_column_name_complies_to_column_type",
            type_pattern=type_pattern,
            types=types,
        ).execute()


@pytest.mark.parametrize(
    ("catalog_node", "column_name_pattern", "models", "expectation"),
    [
        (
            CatalogNodes(
                **{
                    "columns": {
                        "columnone": {
                            "index": 1,
                            "name": "columnone",
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
            ),
            "[a-z]*",
            [
                Nodes4(
                    **{
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "columnone": {
                                "description": None,
                                "index": 1,
                                "name": "columnone",
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
                    },
                ),
            ],
            does_not_raise(),
        ),
        (
            CatalogNodes(
                **{
                    "columns": {
                        "columnone": {
                            "index": 1,
                            "name": "columnone",
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
            ),
            "[A-Z]*",
            [
                Nodes4(
                    **{
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "columnone": {
                                "description": None,
                                "index": 1,
                                "name": "columnone",
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
                    },
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_column_names(
    catalog_node,
    column_name_pattern,
    models,
    expectation,
):
    with expectation:
        CheckColumnNames(
            catalog_node=catalog_node,
            column_name_pattern=column_name_pattern,
            models=models,
            name="check_column_names",
        ).execute()
