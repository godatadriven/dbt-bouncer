from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.catalog.check_columns import (
    check_column_data_must_end_underscore_date,
)


@pytest.mark.parametrize(
    "catalog_node, expectation",
    [
        (
            {
                "columns": {
                    "col_1_date": {
                        "name": "col_1_date",
                        "type": "DATE",
                    },
                },
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "columns": {
                    "col_1_date": {
                        "name": "col_1_date",
                        "type": "DATE",
                    },
                    "col_2_date": {
                        "name": "col_2_date",
                        "type": "DATE",
                    },
                    "col_3": {
                        "name": "col_3",
                        "type": "VARCHAR",
                    },
                },
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "columns": {
                    "col_1": {
                        "name": "col_1",
                        "type": "DATE",
                    },
                },
                "unique_id": "model.package_name.model_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_column_data_must_end_underscore_date(catalog_node, expectation):
    with expectation:
        check_column_data_must_end_underscore_date(catalog_node=catalog_node, request=None)
