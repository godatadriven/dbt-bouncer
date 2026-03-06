from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.models.columns import (
    CheckModelColumnsHaveMetaKeys,
    CheckModelColumnsHaveTypes,
    CheckModelHasConstraints,
)

_TEST_DATA_FOR_CHECK_MODEL_COLUMNS_HAVE_META_KEYS = [
    pytest.param(
        ["owner"],
        {
            "columns": {
                "col_1": {
                    "name": "col_1",
                    "meta": {"owner": "data-team"},
                },
            },
        },
        does_not_raise(),
        id="column_has_required_key",
    ),
    pytest.param(
        ["owner"],
        {
            "columns": {},
        },
        does_not_raise(),
        id="no_columns",
    ),
    pytest.param(
        ["owner"],
        {
            "columns": {
                "col_1": {
                    "name": "col_1",
                    "meta": {},
                },
            },
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="column_missing_required_key",
    ),
    pytest.param(
        ["owner"],
        {
            "columns": {
                "col_1": {
                    "name": "col_1",
                    "meta": {"maturity": "high"},
                },
            },
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="column_has_other_key_but_missing_required",
    ),
]


@pytest.mark.parametrize(
    ("keys", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_COLUMNS_HAVE_META_KEYS,
    indirect=["model"],
)
def test_check_model_columns_have_meta_keys(keys, model, expectation):
    with expectation:
        CheckModelColumnsHaveMetaKeys(
            keys=keys, model=model, name="check_model_columns_have_meta_keys"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_COLUMNS_HAVE_TYPES = [
    pytest.param(
        {
            "columns": {
                "col_1": {
                    "name": "col_1",
                    "data_type": "varchar",
                },
            },
        },
        does_not_raise(),
        id="column_has_type",
    ),
    pytest.param(
        {
            "columns": {},
        },
        does_not_raise(),
        id="no_columns",
    ),
    pytest.param(
        {
            "columns": {
                "col_1": {
                    "name": "col_1",
                },
            },
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="column_missing_type",
    ),
    pytest.param(
        {
            "columns": {
                "col_1": {
                    "name": "col_1",
                    "data_type": "integer",
                },
                "col_2": {
                    "name": "col_2",
                },
            },
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="one_column_missing_type",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_COLUMNS_HAVE_TYPES,
    indirect=["model"],
)
def test_check_model_columns_have_types(model, expectation):
    with expectation:
        CheckModelColumnsHaveTypes(
            model=model, name="check_model_columns_have_types"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_CONSTRAINTS = [
    pytest.param(
        ["primary_key"],
        {
            "config": {"materialized": "table"},
            "constraints": [{"type": "primary_key"}],
        },
        does_not_raise(),
        id="table_has_required_constraint",
    ),
    pytest.param(
        ["primary_key"],
        {
            "config": {"materialized": "view"},
            "constraints": [],
        },
        does_not_raise(),
        id="view_skipped",
    ),
    pytest.param(
        ["primary_key"],
        {
            "config": {"materialized": "ephemeral"},
            "constraints": [],
        },
        does_not_raise(),
        id="ephemeral_skipped",
    ),
    pytest.param(
        ["primary_key"],
        {
            "config": {"materialized": "table"},
            "constraints": [],
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="table_missing_required_constraint",
    ),
    pytest.param(
        ["primary_key", "not_null"],
        {
            "config": {"materialized": "incremental"},
            "constraints": [{"type": "primary_key"}],
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="incremental_missing_one_constraint",
    ),
]


@pytest.mark.parametrize(
    ("required_constraint_types", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_CONSTRAINTS,
    indirect=["model"],
)
def test_check_model_has_constraints(required_constraint_types, model, expectation):
    with expectation:
        CheckModelHasConstraints(
            model=model,
            name="check_model_has_constraints",
            required_constraint_types=required_constraint_types,
        ).execute()
