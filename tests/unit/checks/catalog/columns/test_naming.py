import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckColumnNameCompliesToColumnType:
    def test_valid_date(self):
        check_passes(
            "check_column_name_complies_to_column_type",
            catalog_node={
                "columns": {
                    "col_1_date": {
                        "index": 1,
                        "name": "col_1_date",
                        "type": "DATE",
                    },
                },
            },
            column_name_pattern=".*_date$",
            type_pattern=None,
            types=["DATE"],
        )

    def test_valid_dates(self):
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
            types=["DATE"],
        )

    def test_invalid_struct(self):
        check_fails(
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
                        "type": "STRUCT",
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
            types=None,
        )

    def test_invalid_name_matches_pattern_but_wrong_type(self):
        # Column name ends with `_date` (matches pattern) but the type is
        # VARCHAR (not in `types`), so the rule "name → type" is violated.
        check_fails(
            "check_column_name_complies_to_column_type",
            catalog_node={
                "columns": {
                    "created_date": {
                        "index": 1,
                        "name": "created_date",
                        "type": "VARCHAR",
                    },
                },
            },
            column_name_pattern=".*_date$",
            type_pattern=None,
            types=["DATE"],
        )

    def test_valid_name_does_not_match_pattern(self):
        # A column whose name does not match the pattern is not constrained
        # by this check — its type is irrelevant.
        check_passes(
            "check_column_name_complies_to_column_type",
            catalog_node={
                "columns": {
                    "active": {
                        "index": 1,
                        "name": "active",
                        "type": "BOOLEAN",
                    },
                },
            },
            column_name_pattern="^is_.*",
            type_pattern=None,
            types=["BOOLEAN"],
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
    def test_valid_boolean_with_is_prefix(self):
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
            types=["BOOLEAN"],
        )

    def test_valid_mixed_types(self):
        check_passes(
            "check_column_type_complies_to_column_name",
            catalog_node={
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
            column_name_pattern="^(is|has)_.*",
            type_pattern=None,
            types=["BOOLEAN"],
        )

    def test_valid_type_pattern(self):
        check_passes(
            "check_column_type_complies_to_column_name",
            catalog_node={
                "columns": {
                    "created_date": {
                        "index": 1,
                        "name": "created_date",
                        "type": "DATE",
                    },
                },
            },
            column_name_pattern=".*_date$",
            type_pattern="^DATE",
            types=None,
        )

    def test_invalid_boolean_missing_prefix(self):
        check_fails(
            "check_column_type_complies_to_column_name",
            catalog_node={
                "columns": {
                    "active": {
                        "index": 1,
                        "name": "active",
                        "type": "BOOLEAN",
                    },
                },
            },
            column_name_pattern="^(is|has)_.*",
            type_pattern=None,
            types=["BOOLEAN"],
        )

    def test_invalid_type_pattern(self):
        check_fails(
            "check_column_type_complies_to_column_name",
            catalog_node={
                "columns": {
                    "my_struct": {
                        "index": 1,
                        "name": "my_struct",
                        "type": "STRUCT",
                    },
                },
            },
            column_name_pattern="^struct_.*",
            type_pattern="^STRUCT",
            types=None,
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
    def test_valid_name(self):
        check_passes(
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
            column_name_pattern="[a-z]*",
            ctx_models=[
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
        )

    def test_invalid_name(self):
        check_fails(
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
            column_name_pattern="[A-Z]*",
            ctx_models=[
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
        )
