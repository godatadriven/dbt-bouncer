import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckColumnNameCompliesToColumnType:
    @pytest.mark.parametrize(
        ("catalog_node", "column_name_pattern", "type_pattern", "types", "check_fn"),
        [
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
                check_passes,
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
                check_passes,
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
                check_fails,
                id="invalid_struct",
            ),
            # Column name ends with `_date` (matches pattern) but the type is
            # VARCHAR (not in `types`), so the rule "name → type" is violated.
            pytest.param(
                {
                    "columns": {
                        "created_date": {
                            "index": 1,
                            "name": "created_date",
                            "type": "VARCHAR",
                        },
                    },
                },
                ".*_date$",
                None,
                ["DATE"],
                check_fails,
                id="invalid_name_matches_pattern_but_wrong_type",
            ),
            # A column whose name does not match the pattern is not constrained
            # by this check — its type is irrelevant.
            pytest.param(
                {
                    "columns": {
                        "active": {
                            "index": 1,
                            "name": "active",
                            "type": "BOOLEAN",
                        },
                    },
                },
                "^is_.*",
                None,
                ["BOOLEAN"],
                check_passes,
                id="valid_name_does_not_match_pattern",
            ),
            # This check has no model-only gate, so it also validates columns on
            # seed catalog nodes, not just models.
            pytest.param(
                {
                    "unique_id": "seed.package_name.seed_1",
                    "columns": {
                        "created_date": {
                            "index": 1,
                            "name": "created_date",
                            "type": "VARCHAR",
                        },
                    },
                },
                ".*_date$",
                None,
                ["DATE"],
                check_fails,
                id="seed_catalog_node_is_also_validated",
            ),
        ],
    )
    def test_check_column_name_complies_to_column_type(
        self, catalog_node, column_name_pattern, type_pattern, types, check_fn
    ):
        check_fn(
            "check_column_name_complies_to_column_type",
            catalog_node=catalog_node,
            column_name_pattern=column_name_pattern,
            type_pattern=type_pattern,
            types=types,
        )

    def test_differs_from_reverse_check_with_types(self):
        # Sanity: with the same fixture, the two checks must produce different
        # results. A BOOLEAN column named `active` violates the type→name rule
        # ("BOOLEANs must start with `is_`") but not the name→type rule
        # ("`is_*` columns must be BOOLEAN"), since its name is not `is_*`.
        catalog_node = {
            "columns": {
                "active": {
                    "index": 1,
                    "name": "active",
                    "type": "BOOLEAN",
                },
            },
        }
        check_passes(
            "check_column_name_complies_to_column_type",
            catalog_node=catalog_node,
            column_name_pattern="^is_.*",
            type_pattern=None,
            types=["BOOLEAN"],
        )
        check_fails(
            "check_column_type_complies_to_column_name",
            catalog_node=catalog_node,
            column_name_pattern="^is_.*",
            type_pattern=None,
            types=["BOOLEAN"],
        )

    def test_missing_pattern_and_types(self):
        with pytest.raises(ValueError, match=r"type_pattern.*types.*must be supplied"):
            check_passes(
                "check_column_name_complies_to_column_type",
                catalog_node={
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
                column_name_pattern=".*_date$",
                type_pattern=None,
                types=None,
            )

    def test_both_pattern_and_types(self):
        with pytest.raises(ValueError, match=r"Only one of.*type_pattern.*types"):
            check_passes(
                "check_column_name_complies_to_column_type",
                catalog_node={
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
                column_name_pattern=".*_date$",
                type_pattern="^(?!STRUCT)",
                types=["DATE"],
            )


class TestCheckColumnTypeCompliesToColumnName:
    @pytest.mark.parametrize(
        ("catalog_node", "column_name_pattern", "type_pattern", "types", "check_fn"),
        [
            pytest.param(
                {
                    "columns": {
                        "is_active": {
                            "index": 1,
                            "name": "is_active",
                            "type": "BOOLEAN",
                        },
                    },
                },
                "^(is|has)_.*",
                None,
                ["BOOLEAN"],
                check_passes,
                id="valid_boolean_with_is_prefix",
            ),
            pytest.param(
                {
                    "columns": {
                        "is_active": {
                            "index": 1,
                            "name": "is_active",
                            "type": "BOOLEAN",
                        },
                        "has_email": {
                            "index": 2,
                            "name": "has_email",
                            "type": "BOOLEAN",
                        },
                        "user_name": {
                            "index": 3,
                            "name": "user_name",
                            "type": "VARCHAR",
                        },
                    },
                },
                "^(is|has)_.*",
                None,
                ["BOOLEAN"],
                check_passes,
                id="valid_mixed_types",
            ),
            pytest.param(
                {
                    "columns": {
                        "created_date": {
                            "index": 1,
                            "name": "created_date",
                            "type": "DATE",
                        },
                    },
                },
                ".*_date$",
                "^DATE",
                None,
                check_passes,
                id="valid_type_pattern",
            ),
            pytest.param(
                {
                    "columns": {
                        "active": {
                            "index": 1,
                            "name": "active",
                            "type": "BOOLEAN",
                        },
                    },
                },
                "^(is|has)_.*",
                None,
                ["BOOLEAN"],
                check_fails,
                id="invalid_boolean_missing_prefix",
            ),
            pytest.param(
                {
                    "columns": {
                        "my_struct": {
                            "index": 1,
                            "name": "my_struct",
                            "type": "STRUCT",
                        },
                    },
                },
                "^struct_.*",
                "^STRUCT",
                None,
                check_fails,
                id="invalid_type_pattern",
            ),
        ],
    )
    def test_check_column_type_complies_to_column_name(
        self, catalog_node, column_name_pattern, type_pattern, types, check_fn
    ):
        check_fn(
            "check_column_type_complies_to_column_name",
            catalog_node=catalog_node,
            column_name_pattern=column_name_pattern,
            type_pattern=type_pattern,
            types=types,
        )

    def test_missing_pattern_and_types(self):
        with pytest.raises(ValueError, match=r"type_pattern.*types.*must be supplied"):
            check_passes(
                "check_column_type_complies_to_column_name",
                catalog_node={
                    "columns": {
                        "is_active": {
                            "index": 1,
                            "name": "is_active",
                            "type": "BOOLEAN",
                        },
                    },
                },
                column_name_pattern="^(is|has)_.*",
                type_pattern=None,
                types=None,
            )

    def test_both_pattern_and_types(self):
        with pytest.raises(ValueError, match=r"Only one of.*type_pattern.*types"):
            check_passes(
                "check_column_type_complies_to_column_name",
                catalog_node={
                    "columns": {
                        "is_active": {
                            "index": 1,
                            "name": "is_active",
                            "type": "BOOLEAN",
                        },
                    },
                },
                column_name_pattern="^(is|has)_.*",
                type_pattern="^BOOL",
                types=["BOOLEAN"],
            )


class TestCheckColumnNames:
    @pytest.mark.parametrize(
        ("column_name_pattern", "ctx_models", "check_fn"),
        [
            pytest.param(
                "[a-z]*",
                [
                    {
                        "columns": {
                            "columnone": {
                                "description": None,
                                "index": 1,
                                "name": "columnone",
                                "type": "INTEGER",
                            },
                        },
                    }
                ],
                check_passes,
                id="valid_name",
            ),
            pytest.param(
                "[A-Z]*",
                [
                    {
                        "columns": {
                            "columnone": {
                                "description": None,
                                "index": 1,
                                "name": "columnone",
                                "type": "INTEGER",
                            },
                        },
                    }
                ],
                check_fails,
                id="invalid_name",
            ),
            # `re.fullmatch` requires the entire name to match, not just a prefix
            # (contrast with checks that use `re.match`, e.g. `check_seed_names`).
            pytest.param(
                "col",
                [
                    {
                        "columns": {
                            "columnone": {
                                "description": None,
                                "index": 1,
                                "name": "columnone",
                                "type": "INTEGER",
                            },
                        },
                    }
                ],
                check_fails,
                id="prefix_only_match_fails_fullmatch",
            ),
            pytest.param(
                "[a-z]*",
                [],
                check_passes,
                id="non_model_catalog_node_is_skipped",
            ),
        ],
    )
    def test_check_column_names(self, column_name_pattern, ctx_models, check_fn):
        check_fn(
            "check_column_names",
            catalog_node={
                "columns": {
                    "columnone": {
                        "index": 1,
                        "name": "columnone",
                        "type": "DATE",
                    },
                },
            },
            column_name_pattern=column_name_pattern,
            ctx_models=ctx_models,
        )

    def test_check_column_names_invalid_regex(self):
        import re

        from dbt_bouncer.testing import _run_check

        with pytest.raises(re.error):
            _run_check(
                "check_column_names",
                catalog_node={
                    "columns": {"columnone": {"index": 1, "name": "columnone"}},
                },
                column_name_pattern="(unclosed",
                ctx_models=[
                    {
                        "columns": {
                            "columnone": {"index": 1, "name": "columnone"},
                        },
                    }
                ],
            )
