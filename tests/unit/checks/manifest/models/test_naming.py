from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.models.naming import (
    CheckModelNames,
)

_TEST_DATA_FOR_CHECK_MODEL_NAMES = [
    pytest.param(
        "",
        "^stg_",
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        does_not_raise(),
        id="valid_name_stg",
    ),
    pytest.param(
        "^staging",
        "^stg_",
        {
            "alias": "stg_model_2",
            "fqn": ["package_name", "stg_model_2"],
            "name": "stg_model_2",
            "original_file_path": "models/staging/stg_model_2.sql",
            "path": "staging/stg_model_2.sql",
            "unique_id": "model.package_name.stg_model_2",
        },
        does_not_raise(),
        id="valid_name_staging_dir",
    ),
    pytest.param(
        "^intermediate",
        "^stg_",
        {
            "alias": "stg_model_3",
            "fqn": ["package_name", "stg_model_3"],
            "name": "stg_model_3",
            "original_file_path": "models/staging/stg_model_3.sql",
            "path": "staging/stg_model_3.sql",
            "unique_id": "model.package_name.stg_model_3",
        },
        does_not_raise(),
        id="valid_name_ignored_dir",
    ),
    pytest.param(
        "^intermediate",
        "^int_",
        {
            "alias": "int_model_1",
            "fqn": ["package_name", "int_model_1"],
            "name": "int_model_1",
            "original_file_path": "models/intermediate/int_model_1.sql",
            "path": "intermediate/int_model_1.sql",
            "unique_id": "model.package_name.int_model_1",
        },
        does_not_raise(),
        id="valid_name_int",
    ),
    pytest.param(
        "^intermediate",
        "^int_",
        {
            "alias": "model_1",
            "fqn": ["package_name", "model_1"],
            "name": "model_1",
            "original_file_path": "models/intermediate/model_1.sql",
            "path": "intermediate/model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_name_int",
    ),
    pytest.param(
        "^intermediate",
        "^int_",
        {
            "alias": "model_int_2",
            "fqn": ["package_name", "model_int_2"],
            "name": "model_int_2",
            "original_file_path": "models/intermediate/model_int_2.sql",
            "path": "intermediate/model_int_2.sql",
            "unique_id": "model.package_name.model_int_2",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_name_int_suffix",
    ),
]


@pytest.mark.parametrize(
    ("include", "model_name_pattern", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_NAMES,
    indirect=["model"],
)
def test_check_mode_names(include, model_name_pattern, model, expectation):
    with expectation:
        CheckModelNames(
            include=include,
            model_name_pattern=model_name_pattern,
            model=model,
            name="check_model_names",
        ).execute()
