import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckColumnHasSpecifiedTest:
    @pytest.mark.parametrize(
        ("catalog_node", "column_name_pattern", "test_name", "ctx_tests", "check_fn"),
        [
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
                [{}],
                check_passes,
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
                [
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
                check_fails,
                id="missing_test",
            ),
            pytest.param(
                # With the default case-sensitive comparison on a non-folding adapter,
                # an upper-cased catalog column does not match its lowercase test entry.
                {
                    "columns": {
                        "COL_1": {
                            "index": 1,
                            "name": "COL_1",
                            "type": "INTEGER",
                        },
                    },
                },
                ".*_1$",
                "unique",
                [{}],
                check_fails,
                id="case_mismatch_fails_when_case_sensitive",
            ),
            pytest.param(
                # The test matches on name and column, but is attached to a
                # different node entirely, so it must not count towards this one.
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
                [{"attached_node": "model.package_name.model_2"}],
                check_fails,
                id="test_attached_to_different_node",
            ),
            pytest.param(
                # A model-level test (no column_name) must not satisfy a
                # column-level test requirement.
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
                [{"column_name": ""}],
                check_fails,
                id="model_level_test_does_not_satisfy_column_requirement",
            ),
        ],
    )
    def test_check_column_has_specified_test(
        self,
        catalog_node,
        column_name_pattern,
        test_name,
        ctx_tests,
        check_fn,
    ):
        # ``case_sensitive`` is intentionally omitted so these cases exercise the
        # check's default (``True``) on a non-folding adapter.
        check_fn(
            "check_column_has_specified_test",
            catalog_node=catalog_node,
            column_name_pattern=column_name_pattern,
            test_name=test_name,
            ctx_tests=ctx_tests,
        )

    def test_check_column_has_specified_test_case_insensitive(self):
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
            ctx_tests=[{}],
            case_sensitive=False,
        )

    @pytest.mark.parametrize(
        ("catalog_node", "column_name_pattern", "test_name", "ctx_tests", "check_fn"),
        [
            pytest.param(
                # On Snowflake the catalog column is upper-cased (`COL_1`) while the
                # test's `column_name` mirrors the lowercase YAML (`col_1`); the check
                # should still match because `case_sensitive` defaults to `false` for
                # the snowflake adapter.
                {
                    "columns": {
                        "COL_1": {
                            "index": 1,
                            "name": "COL_1",
                            "type": "INTEGER",
                        },
                    },
                },
                ".*_1$",
                "unique",
                [{}],
                check_passes,
                id="has_test_snowflake",
            ),
            pytest.param(
                # On Snowflake an upper-cased catalog column (`IS_ACTIVE`) must still be
                # selected by a conventionally lowercase pattern (`^is_.*`), otherwise
                # the column silently falls out of scope and the check passes vacuously
                # rather than flagging the missing test.
                {
                    "columns": {
                        "IS_ACTIVE": {
                            "index": 1,
                            "name": "IS_ACTIVE",
                            "type": "BOOLEAN",
                        },
                    },
                },
                "^is_.*",
                "not_null",
                [{}],
                check_fails,
                id="lowercase_pattern_matches_upper_catalog_column_snowflake",
            ),
        ],
    )
    def test_check_column_has_specified_test_snowflake(
        self,
        catalog_node,
        column_name_pattern,
        test_name,
        ctx_tests,
        check_fn,
    ):
        check_fn(
            "check_column_has_specified_test",
            catalog_node=catalog_node,
            column_name_pattern=column_name_pattern,
            test_name=test_name,
            ctx_tests=ctx_tests,
            ctx_manifest_obj={"metadata": {"adapter_type": "snowflake"}},
        )
