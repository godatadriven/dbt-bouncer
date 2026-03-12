from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.check_context import CheckContext
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.models.versioning import (
    CheckModelLatestVersionSpecified,
    CheckModelVersionAllowed,
    CheckModelVersionPinnedInRef,
)

_TEST_DATA_FOR_CHECK_MODEL_LATEST_VERSION_SPECIFIED = [
    pytest.param(
        {
            "latest_version": 2,
        },
        does_not_raise(),
        id="latest_version_integer",
    ),
    pytest.param(
        {
            "latest_version": "stable",
        },
        does_not_raise(),
        id="latest_version_string",
    ),
    pytest.param(
        {
            "latest_version": None,
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_latest_version",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_LATEST_VERSION_SPECIFIED,
    indirect=["model"],
)
def test_check_model_latest_version_specified(
    model,
    expectation,
):
    with expectation:
        CheckModelLatestVersionSpecified(
            model=model,
            name="check_model_latest_version_specified",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_VERSION_ALLOWED = [
    pytest.param(
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": 1,
        },
        r"[0-9]\d*",
        does_not_raise(),
        id="allowed_version_1",
    ),
    pytest.param(
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": 10,
        },
        r"[0-9]\d*",
        does_not_raise(),
        id="allowed_version_10",
    ),
    pytest.param(
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": 100,
        },
        r"[0-9]\d*",
        does_not_raise(),
        id="allowed_version_100",
    ),
    pytest.param(
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": "golden",
        },
        r"[0-9]\d*",
        pytest.raises(DbtBouncerFailedCheckError),
        id="disallowed_version",
    ),
]


@pytest.mark.parametrize(
    ("model", "version_pattern", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_VERSION_ALLOWED,
    indirect=["model"],
)
def test_check_model_version_allowed(model, version_pattern, expectation):
    with expectation:
        CheckModelVersionAllowed(
            model=model,
            name="check_model_version_allowed",
            version_pattern=version_pattern,
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_VERSION_PINNED_IN_REF = [
    pytest.param(
        {
            "manifest": {
                "child_map": {
                    "model.package_name.stg_model_1": ["model.package_name.stg_model_2"]
                },
                "nodes": {
                    "model.package_name.stg_model_1": {
                        "alias": "stg_model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "stg_model_1"],
                        "name": "stg_model_1",
                        "original_file_path": "models/staging/stg_model_1.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_1.sql",
                        "resource_type": "model",
                        "schema": "dbt_jdoe_stg_domain",
                        "unique_id": "model.package_name.stg_model_1",
                        "version": 1,
                    },
                    "model.package_name.stg_model_2": {
                        "alias": "stg_model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "stg_model_2"],
                        "name": "stg_model_2",
                        "original_file_path": "models/staging/stg_model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_2.sql",
                        "refs": [{"name": "stg_model_1", "version": 1}],
                        "resource_type": "model",
                        "schema": "dbt_jdoe_stg_domain",
                        "unique_id": "model.package_name.stg_model_2",
                        "version": 1,
                    },
                },
            }
        },
        {
            "alias": "stg_model_1",
            "checksum": {"name": "sha256", "checksum": ""},
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
            },
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "package_name": "package_name",
            "path": "staging/stg_model_1.sql",
            "resource_type": "model",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": 1,
        },
        does_not_raise(),
        id="pinned_version",
    ),
    pytest.param(
        {
            "manifest": {
                "child_map": {
                    "model.package_name.stg_model_1": ["model.package_name.stg_model_2"]
                },
                "nodes": {
                    "model.package_name.stg_model_1": {
                        "alias": "stg_model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "stg_model_1"],
                        "name": "stg_model_1",
                        "original_file_path": "models/staging/stg_model_1.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_1.sql",
                        "resource_type": "model",
                        "schema": "dbt_jdoe_stg_domain",
                        "unique_id": "model.package_name.stg_model_1",
                        "version": 1,
                    },
                    "model.package_name.stg_model_2": {
                        "alias": "stg_model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "stg_model_2"],
                        "name": "stg_model_2",
                        "original_file_path": "models/staging/stg_model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_2.sql",
                        "refs": [{"name": "stg_model_1", "version": None}],
                        "resource_type": "model",
                        "schema": "dbt_jdoe_stg_domain",
                        "unique_id": "model.package_name.stg_model_2",
                        "version": 1,
                    },
                },
            }
        },
        {
            "alias": "stg_model_1",
            "checksum": {"name": "sha256", "checksum": ""},
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
            },
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "package_name": "package_name",
            "path": "staging/stg_model_1.sql",
            "resource_type": "model",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": 1,
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="unpinned_version",
    ),
]


@pytest.mark.parametrize(
    ("manifest_obj", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_VERSION_PINNED_IN_REF,
    indirect=["manifest_obj", "model"],
)
def test_check_model_version_pinned_in_ref(manifest_obj, model, expectation):
    with expectation:
        check = CheckModelVersionPinnedInRef(
            model=model,
            name="check_model_version_pinned_in_ref",
        )
        check._ctx = CheckContext(manifest_obj=manifest_obj)
        check.execute()
