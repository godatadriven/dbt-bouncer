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

    def test_has_test_snowflake(self):
        # On Snowflake the catalog column is upper-cased (`COL_1`) while the test's
        # `column_name` mirrors the lowercase YAML (`col_1`); the check should still
        # match because `case_sensitive` defaults to `false` for the snowflake adapter.
        check_passes(
            "check_column_has_specified_test",
            catalog_node={
                "columns": {
                    "COL_1": {
                        "index": 1,
                        "name": "COL_1",
                        "type": "INTEGER",
                    },
                },
            },
            column_name_pattern=".*_1$",
            test_name="unique",
            ctx_tests=[{}],
            ctx_manifest_obj={"metadata": {"adapter_type": "snowflake"}},
        )

    def test_has_test_case_insensitive_explicit(self):
        # Casing mismatch resolved by explicitly opting into case-insensitive matching.
        check_passes(
            "check_column_has_specified_test",
            catalog_node={
                "columns": {
                    "COL_1": {
                        "index": 1,
                        "name": "COL_1",
                        "type": "INTEGER",
                    },
                },
            },
            column_name_pattern=".*_1$",
            test_name="unique",
            case_sensitive=False,
            ctx_tests=[{}],
        )

    def test_case_mismatch_fails_when_case_sensitive(self):
        # With the default case-sensitive comparison on a non-folding adapter, an
        # upper-cased catalog column does not match its lowercase test entry.
        check_fails(
            "check_column_has_specified_test",
            catalog_node={
                "columns": {
                    "COL_1": {
                        "index": 1,
                        "name": "COL_1",
                        "type": "INTEGER",
                    },
                },
            },
            column_name_pattern=".*_1$",
            test_name="unique",
            ctx_tests=[{}],
        )

    def test_lowercase_pattern_matches_upper_catalog_column_snowflake(self):
        # On Snowflake an upper-cased catalog column (`IS_ACTIVE`) must still be
        # selected by a conventionally lowercase pattern (`^is_.*`), otherwise the
        # column silently falls out of scope and the check passes vacuously rather
        # than flagging the missing test.
        check_fails(
            "check_column_has_specified_test",
            catalog_node={
                "columns": {
                    "IS_ACTIVE": {
                        "index": 1,
                        "name": "IS_ACTIVE",
                        "type": "BOOLEAN",
                    },
                },
            },
            column_name_pattern="^is_.*",
            test_name="not_null",
            ctx_tests=[{}],
            ctx_manifest_obj={"metadata": {"adapter_type": "snowflake"}},
        )
