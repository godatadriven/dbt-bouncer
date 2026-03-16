from dbt_bouncer.testing import check_fails, check_passes


class TestCheckColumnHasSpecifiedTest:
    def test_has_test(self):
        check_passes(
            "check_column_has_specified_test",
            catalog_node={
                "columns": {
                    "col_1": {
                        "index": 1,
                        "name": "col_1",
                        "type": "INTEGER",
                    },
                },
            },
            column_name_pattern=".*_1$",
            test_name="unique",
            ctx_tests=[{}],
        )

    def test_missing_test(self):
        check_fails(
            "check_column_has_specified_test",
            catalog_node={
                "columns": {
                    "col_1": {
                        "index": 1,
                        "name": "col_1",
                        "type": "INTEGER",
                    },
                },
            },
            column_name_pattern=".*_1$",
            test_name="unique",
            ctx_tests=[
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
                }
            ],
        )
