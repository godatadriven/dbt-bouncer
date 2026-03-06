from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.models.directories import (
    CheckModelDirectories,
    CheckModelFileName,
    CheckModelPropertyFileLocation,
    CheckModelSchemaName,
)

_TEST_DATA_FOR_CHECK_MODEL_DIRECTORIES = [
    pytest.param(
        "models",
        {
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
        },
        ["staging", "mart", "intermediate"],
        does_not_raise(),
        id="valid_directory",
    ),
    pytest.param(
        "models/marts",
        {
            "original_file_path": "models/marts/finance/marts_model_1.sql",
            "path": "marts/finance/marts_model_1.sql",
        },
        ["finance", "marketing"],
        does_not_raise(),
        id="valid_subdirectory",
    ),
    pytest.param(
        "models/marts/",
        {
            "original_file_path": "models/marts/finance/marts_model_1.sql",
            "path": "marts/finance/marts_model_1.sql",
        },
        ["finance", "marketing"],
        does_not_raise(),
        id="valid_subdirectory_trailing_slash",
    ),
    pytest.param(
        "models/marts",
        {
            "original_file_path": "models/marts/sales/marts_model_1.sql",
            "path": "marts/sales/marts_model_1.sql",
        },
        ["finance", "marketing"],
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_subdirectory",
    ),
    pytest.param(
        "models",
        {
            "original_file_path": "models/model_1.sql",
            "path": "marts/sales/model_1.sql",
        },
        ["finance", "marketing"],
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_root_directory",
    ),
]


@pytest.mark.parametrize(
    ("include", "model", "permitted_sub_directories", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DIRECTORIES,
    indirect=["model"],
)
def test_check_model_directories(
    include,
    model,
    permitted_sub_directories,
    expectation,
):
    with expectation:
        CheckModelDirectories(
            include=include,
            model=model,
            name="check_model_directories",
            permitted_sub_directories=permitted_sub_directories,
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_FILE_NAMES = [
    pytest.param(
        r".*(v[0-9])\.sql$",
        {
            "alias": "model_v1",
            "config": {"grants": {"select": ["user1"]}},
            "fqn": ["package_name", "model_v1"],
            "name": "model_v1",
            "original_file_path": "model_v1.sql",
            "path": "staging/finance/model_v1.sql",
            "unique_id": "model.package_name.model_v1",
        },
        does_not_raise(),
        id="valid_file_name",
    ),
    pytest.param(
        ".*(v[0-9])$",
        {
            "alias": "model_v1",
            "config": {"grants": {"select": ["user1"]}},
            "fqn": ["package_name", "model_v1"],
            "name": "model_v1",
            "original_file_path": "model_v1.sql",
            "path": "staging/finance/model_v1.sql",
            "unique_id": "model.package_name.model_v1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_file_name",
    ),
]


@pytest.mark.parametrize(
    ("file_name_pattern", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_FILE_NAMES,
    indirect=["model"],
)
def test_check_model_file_names(file_name_pattern, model, expectation):
    with expectation:
        CheckModelFileName(
            file_name_pattern=file_name_pattern,
            model=model,
            name="check_model_file_name",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_PROPERTY_FILE_LOCATION = [
    pytest.param(
        {
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        does_not_raise(),
        id="valid_location_stg",
    ),
    pytest.param(
        {
            "original_file_path": "models/intermediate/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_int_crm__models.yml",
            "path": "intermediate/crm/stg_model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        does_not_raise(),
        id="valid_location_int",
    ),
    pytest.param(
        {
            "original_file_path": "models/marts/crm/stg_model_1.sql",
            "patch_path": "package_name://models/marts/crm/_crm__models.yml",
            "path": "marts/crm/stg_model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        does_not_raise(),
        id="valid_location_marts",
    ),
    pytest.param(
        {
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_staging_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_prefix",
    ),
    pytest.param(
        {
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "resource_type": "model",
            "unique_id": "model.package_name.model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_underscore",
    ),
    pytest.param(
        {
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_schema.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_name",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_PROPERTY_FILE_LOCATION,
    indirect=["model"],
)
def test_check_model_property_file_location(model, expectation):
    with expectation:
        CheckModelPropertyFileLocation(
            model=model, name="check_model_property_file_location"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_SCHEMA_NAME = [
    pytest.param(
        "",
        ".*stg_.*",
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
        },
        does_not_raise(),
        id="valid_schema_stg",
    ),
    pytest.param(
        "^staging",
        "stg_",
        {
            "alias": "stg_model_2",
            "fqn": ["package_name", "stg_model_2"],
            "name": "stg_model_2",
            "original_file_path": "models/staging/stg_model_2.sql",
            "path": "staging/stg_model_2.sql",
            "schema": "stg_domain",
            "unique_id": "model.package_name.stg_model_2",
        },
        does_not_raise(),
        id="valid_schema_staging_dir",
    ),
    pytest.param(
        "^intermediate",
        ".*_intermediate",
        {
            "alias": "stg_model_3",
            "fqn": ["package_name", "stg_model_3"],
            "name": "stg_model_3",
            "original_file_path": "models/staging/stg_model_3.sql",
            "path": "staging/stg_model_3.sql",
            "schema": "dbt_jdoe_intermediate",
            "unique_id": "model.package_name.stg_model_3",
        },
        does_not_raise(),
        id="valid_schema_ignored_dir",
    ),
    pytest.param(
        "^intermediate",
        ".*intermediate",
        {
            "alias": "model_1",
            "fqn": ["package_name", "model_1"],
            "name": "model_1",
            "original_file_path": "models/intermediate/model_1.sql",
            "path": "intermediate/model_1.sql",
            "schema": "dbt_jdoe_int_domain",
            "unique_id": "model.package_name.model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_schema_int",
    ),
]


@pytest.mark.parametrize(
    ("include", "schema_name_pattern", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_SCHEMA_NAME,
    indirect=["model"],
)
def test_check_model_schema_name(include, schema_name_pattern, model, expectation):
    with expectation:
        CheckModelSchemaName(
            include=include,
            schema_name_pattern=schema_name_pattern,
            model=model,
            name="check_model_schema_name",
        ).execute()
