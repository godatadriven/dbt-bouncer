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

    def test_invalid_name(self):
        check_fails(
            "check_column_name_complies_to_column_type",
            catalog_node={
                "columns": {
                    "col_1": {
                        "index": 1,
                        "name": "col_1",
                        "type": "DATE",
                    },
                },
            },
            column_name_pattern=".*_date$",
            type_pattern=None,
            types=["DATE"],
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
