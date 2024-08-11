from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.catalog.check_catalog_sources import (
    check_source_columns_are_all_documented,
)


@pytest.mark.parametrize(
    "catalog_source, sources, expectation",
    [
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
                "unique_id": "source.package_name.source_1.table_1",
            },
            [
                {
                    "columns": {
                        "col_1": {
                            "name": "col_1",
                        },
                        "col_2": {
                            "name": "col_2",
                        },
                    },
                    "unique_id": "source.package_name.source_1.table_1",
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
                "unique_id": "source.package_name.source_1.table_1",
            },
            [
                {
                    "columns": {
                        "col_1": {
                            "name": "col_1",
                        },
                    },
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_columns_are_all_documented(catalog_source, sources, expectation):
    with expectation:
        check_source_columns_are_all_documented(
            catalog_source=catalog_source, sources=sources, request=None
        )
