from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.catalog.columns.tests import (
    CheckColumnHasSpecifiedTest,
)
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

_TEST_DATA_FOR_CHECK_COLUMN_HAS_SPECIFIED_TEST = [
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
        ".*_1$",
        "unique",
        {},
        does_not_raise(),
        id="has_test",
    ),
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
        ".*_1$",
        "unique",
        {
            "alias": "not_null_model_1_not_null",
            "fqn": [
                "package_name",
                "marts",
                "finance",
                "not_null_model_1_not_null",
            ],
            "name": "not_null_model_1_not_null",
            "test_metadata": {
                "name": "not_null",
            },
            "unique_id": "test.package_name.not_null_model_1_not_null.cf6c17daed",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_test",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "column_name_pattern", "test_name", "tests", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMN_HAS_SPECIFIED_TEST,
    indirect=["catalog_node", "tests"],
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
