from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.catalog.columns.description import (
    CheckColumnDescriptionPopulated,
    CheckColumnsAreAllDocumented,
    CheckColumnsAreDocumentedInPublicModels,
)
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

_TEST_DATA_FOR_CHECK_COLUMN_DESCRIPTION_POPULATED = [
    pytest.param(
        {},
        {},
        {},
        does_not_raise(),
        id="all_documented",
    ),
    pytest.param(
        {
            "unique_id": "model.package_name.model_2",
        },
        {
            "alias": "model_2",
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
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        {},
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_documentation",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "models", "manifest_obj", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMN_DESCRIPTION_POPULATED,
    indirect=["catalog_node", "models", "manifest_obj"],
)
def test_check_column_description_populated(
    catalog_node, models, manifest_obj, expectation
):
    with expectation:
        CheckColumnDescriptionPopulated(
            catalog_node=catalog_node,
            models=models,
            manifest_obj=manifest_obj,
            name="check_column_description_populated",
        ).execute()


_TEST_DATA_FOR_CHECK_COLUMN_DESCRIPTION_POPULATED_SNOWFLAKE = [
    pytest.param(
        {
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                    "comment": "This is a description",
                },
                "col_2": {
                    "index": 2,
                    "name": "col_2",
                    "type": "INTEGER",
                    "comment": "This is a description",
                },
            },
        },
        {},
        {"adapter_type": "snowflake"},
        does_not_raise(),
        id="all_documented_snowflake",
    ),
    pytest.param(
        {
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                    "comment": "This is a description",
                },
                "col_2": {
                    "index": 2,
                    "name": "col_2",
                    "type": "INTEGER",
                },
            },
        },
        {},
        {"adapter_type": "snowflake"},
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_documentation_snowflake",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "models", "manifest_obj", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMN_DESCRIPTION_POPULATED_SNOWFLAKE,
    indirect=["catalog_node", "models", "manifest_obj"],
)
def test_check_column_description_populated_snowflake(
    catalog_node, models, manifest_obj, expectation
):
    with expectation:
        CheckColumnDescriptionPopulated(
            catalog_node=catalog_node,
            models=models,
            manifest_obj=manifest_obj,
            name="check_column_description_populated",
        ).execute()


_TEST_DATA_FOR_CHECK_COLUMNS_ARE_ALL_DOCUMENTED = [
    pytest.param(
        {},
        {},
        {},
        does_not_raise(),
        id="all_documented",
    ),
    pytest.param(
        {},
        {},
        {
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
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="case_mismatch",
    ),
    pytest.param(
        {
            "unique_id": "model.package_name.model_2",
        },
        {},
        {
            "alias": "model_2",
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
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_column_in_model",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "manifest_obj", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMNS_ARE_ALL_DOCUMENTED,
    indirect=["catalog_node", "manifest_obj", "models"],
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


_TEST_DATA_FOR_CHECK_COLUMNS_ARE_ALL_DOCUMENTED_SNOWFLAKE = [
    pytest.param(
        {},
        {"adapter_type": "snowflake"},
        {
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
        },
        does_not_raise(),
        id="all_documented_snowflake",
    )
]


@pytest.mark.parametrize(
    ("catalog_node", "manifest_obj", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMNS_ARE_ALL_DOCUMENTED_SNOWFLAKE,
    indirect=["catalog_node", "manifest_obj", "models"],
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


_TEST_DATA_FOR_CHECK_COLUMNS_ARE_DOCUMENTED_IN_PUBLIC_MODELS = [
    pytest.param(
        {
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
            },
        },
        {
            "access": "public",
            "columns": {
                "col_1": {
                    "description": "This is a description",
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
            },
        },
        does_not_raise(),
        id="documented_public",
    ),
    pytest.param(
        {},
        {
            "access": "public",
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
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="undocumented_public",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMNS_ARE_DOCUMENTED_IN_PUBLIC_MODELS,
    indirect=["catalog_node", "models"],
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
