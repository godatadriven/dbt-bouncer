from contextlib import nullcontext as does_not_raise

import pytest
from pydantic import ValidationError

from dbt_bouncer.check_context import CheckContext
from dbt_bouncer.checks.catalog.columns.naming import (
    CheckColumnNameCompliesToColumnType,
    CheckColumnNames,
)
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

_TEST_DATA_FOR_CHECK_COLUMN_NAME_COMPLIES_TO_COLUMN_TYPE = [
    pytest.param(
        {
            "columns": {
                "col_1_date": {
                    "index": 1,
                    "name": "col_1_date",
                    "type": "DATE",
                },
            },
        },
        ".*_date$",
        None,
        ["DATE"],
        does_not_raise(),
        id="valid_date",
    ),
    pytest.param(
        {
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
        },
        ".*_date$",
        None,
        ["DATE"],
        does_not_raise(),
        id="valid_dates",
    ),
    pytest.param(
        {
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
        },
        ".*_date$",
        "^(?!STRUCT)",
        None,
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_struct",
    ),
    pytest.param(
        {
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "DATE",
                },
            },
        },
        ".*_date$",
        None,
        ["DATE"],
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_name",
    ),
    pytest.param(
        {
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
        },
        ".*_date$",
        None,
        None,
        pytest.raises(ValidationError),
        id="missing_pattern_and_types",
    ),
    pytest.param(
        {
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
        },
        ".*_date$",
        "^(?!STRUCT)",
        ["DATE"],
        pytest.raises(ValidationError),
        id="both_pattern_and_types",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "column_name_pattern", "type_pattern", "types", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMN_NAME_COMPLIES_TO_COLUMN_TYPE,
    indirect=["catalog_node"],
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


_TEST_DATA_FOR_CHECK_COLUMN_NAMES = [
    pytest.param(
        {
            "columns": {
                "columnone": {
                    "index": 1,
                    "name": "columnone",
                    "type": "DATE",
                },
            },
        },
        "[a-z]*",
        {
            "columns": {
                "columnone": {
                    "description": None,
                    "index": 1,
                    "name": "columnone",
                    "type": "INTEGER",
                },
            },
        },
        does_not_raise(),
        id="valid_name",
    ),
    pytest.param(
        {
            "columns": {
                "columnone": {
                    "index": 1,
                    "name": "columnone",
                    "type": "DATE",
                },
            },
        },
        "[A-Z]*",
        {
            "columns": {
                "columnone": {
                    "description": None,
                    "index": 1,
                    "name": "columnone",
                    "type": "INTEGER",
                },
            },
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_name",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "column_name_pattern", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMN_NAMES,
    indirect=["catalog_node", "models"],
)
def test_check_column_names(
    catalog_node,
    column_name_pattern,
    models,
    expectation,
):
    with expectation:
        check = CheckColumnNames(
            catalog_node=catalog_node,
            column_name_pattern=column_name_pattern,
            name="check_column_names",
        )
        check._ctx = CheckContext(models=models)
        check.execute()
