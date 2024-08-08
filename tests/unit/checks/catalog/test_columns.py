from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.catalog.check_columns import (
    check_column_name_complies_to_column_type,
    check_columns_are_documented_in_public_models,
)


@pytest.mark.parametrize(
    "catalog_node, models, expectation",
    [
        (
            {
                "columns": {
                    "col_1": {
                        "name": "col_1",
                    },
                },
                "unique_id": "model.package_name.model_1",
            },
            [
                {
                    "access": "public",
                    "columns": {
                        "col_1": {
                            "description": "This is a description",
                            "name": "col_1",
                        },
                    },
                    "unique_id": "model.package_name.model_1",
                }
            ],
            does_not_raise(),
        ),
        (
            {
                "columns": {
                    "col_1": {
                        "name": "col_1",
                    },
                    "col_2": {
                        "name": "col_2",
                    },
                },
                "unique_id": "model.package_name.model_1",
            },
            [
                {
                    "access": "public",
                    "columns": {
                        "col_1": {
                            "description": "This is a description",
                            "name": "col_1",
                        },
                        "col_2": {
                            "description": "",
                            "name": "col_2",
                        },
                    },
                    "unique_id": "model.package_name.model_1",
                }
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
            {
                "columns": {
                    "col_1_date": {
                        "name": "col_1_date",
                        "type": "DATE",
                    },
                },
                "unique_id": "model.package_name.model_1",
            },
            {"column_name_pattern": ".*_date$", "types": ["DATE"]},
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
            {"column_name_pattern": ".*_date$", "types": ["DATE"]},
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
