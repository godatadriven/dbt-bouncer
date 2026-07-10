import re

import pytest

from dbt_bouncer.testing import _run_check, check_fails, check_passes


class TestCheckSeedColumnNames:
    @pytest.mark.parametrize(
        ("seed_overrides", "seed_column_name_pattern", "check_fn"),
        [
            pytest.param(
                {
                    "alias": "raw_customers",
                    "columns": {
                        "id": {"name": "id"},
                        "first_name": {"name": "first_name"},
                        "last_name": {"name": "last_name"},
                    },
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "path": "raw_customers.csv",
                    "unique_id": "seed.package_name.raw_customers",
                },
                "^[a-z_]+$",
                check_passes,
                id="all_columns_match_pattern",
            ),
            pytest.param(
                {
                    "alias": "raw_customers",
                    "columns": {
                        "id": {"name": "id"},
                        "firstName": {"name": "firstName"},
                        "last_name": {"name": "last_name"},
                    },
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "path": "raw_customers.csv",
                    "unique_id": "seed.package_name.raw_customers",
                },
                "^[a-z_]+$",
                check_fails,
                id="camelCase_column_name",
            ),
            pytest.param(
                {
                    "alias": "raw_customers",
                    "columns": {},
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "path": "raw_customers.csv",
                    "unique_id": "seed.package_name.raw_customers",
                },
                "^[a-z_]+$",
                check_passes,
                id="no_columns_vacuously_passes",
            ),
        ],
    )
    def test_check_seed_column_names(
        self, seed_overrides, seed_column_name_pattern, check_fn
    ):
        check_fn(
            "check_seed_column_names",
            seed=seed_overrides,
            seed_column_name_pattern=seed_column_name_pattern,
        )

    def test_check_seed_column_names_invalid_regex(self):
        with pytest.raises(re.error):
            _run_check(
                "check_seed_column_names",
                seed={"columns": {"id": {"name": "id"}}},
                seed_column_name_pattern="(unclosed",
            )


class TestCheckSeedColumnsHaveTypes:
    @pytest.mark.parametrize(
        ("seed_overrides", "check_fn"),
        [
            pytest.param(
                {
                    "alias": "raw_customers",
                    "columns": {
                        "id": {"name": "id", "data_type": "integer"},
                        "first_name": {"name": "first_name", "data_type": "varchar"},
                        "last_name": {"name": "last_name", "data_type": "varchar"},
                    },
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "path": "raw_customers.csv",
                    "unique_id": "seed.package_name.raw_customers",
                },
                check_passes,
                id="all_columns_have_types",
            ),
            pytest.param(
                {
                    "alias": "raw_customers",
                    "columns": {
                        "id": {"name": "id", "data_type": "integer"},
                        "first_name": {"name": "first_name"},
                        "last_name": {"name": "last_name", "data_type": "varchar"},
                    },
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "path": "raw_customers.csv",
                    "unique_id": "seed.package_name.raw_customers",
                },
                check_fails,
                id="missing_data_type",
            ),
            pytest.param(
                {
                    "alias": "raw_customers",
                    "columns": {
                        "id": {"name": "id", "data_type": "integer"},
                        "first_name": {"name": "first_name", "data_type": ""},
                    },
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "path": "raw_customers.csv",
                    "unique_id": "seed.package_name.raw_customers",
                },
                check_fails,
                id="empty_string_data_type",
            ),
            pytest.param(
                {
                    "alias": "raw_customers",
                    "columns": {},
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "path": "raw_customers.csv",
                    "unique_id": "seed.package_name.raw_customers",
                },
                check_passes,
                id="no_columns_vacuously_passes",
            ),
        ],
    )
    def test_check_seed_columns_have_types(self, seed_overrides, check_fn):
        check_fn(
            "check_seed_columns_have_types",
            seed=seed_overrides,
        )


_SEED_BASE = {
    "alias": "raw_customers",
    "columns": {},
    "fqn": ["package_name", "raw_customers"],
    "name": "raw_customers",
    "original_file_path": "seeds/raw_customers.csv",
    "path": "raw_customers.csv",
    "unique_id": "seed.package_name.raw_customers",
}


class TestCheckSeedDescriptionPopulated:
    @pytest.mark.parametrize(
        ("seed_overrides", "check_fn"),
        [
            pytest.param(
                {
                    **_SEED_BASE,
                    "description": "Description that is more than 4 characters.",
                },
                check_passes,
                id="description_populated",
            ),
            pytest.param(
                {
                    **_SEED_BASE,
                    "description": """A
                                multiline
                                description
                                """,
                },
                check_passes,
                id="multiline_description_populated",
            ),
            pytest.param(
                {**_SEED_BASE, "description": ""},
                check_fails,
                id="empty_description",
            ),
            pytest.param(
                {**_SEED_BASE, "description": " "},
                check_fails,
                id="whitespace_description",
            ),
            pytest.param(
                {
                    **_SEED_BASE,
                    "description": """
                                """,
                },
                check_fails,
                id="multiline_whitespace_description",
            ),
            pytest.param(
                {**_SEED_BASE, "description": "-"},
                check_fails,
                id="hyphen_description",
            ),
            pytest.param(
                {**_SEED_BASE, "description": "null"},
                check_fails,
                id="null_string_description",
            ),
            pytest.param(
                {**_SEED_BASE, "description": "n/a"},
                check_fails,
                id="n_a_description",
            ),
            pytest.param(
                {**_SEED_BASE, "description": "N/A"},
                check_fails,
                id="uppercase_n_a_description",
            ),
            pytest.param(
                {**_SEED_BASE, "description": "none"},
                check_fails,
                id="none_string_description",
            ),
            pytest.param(
                {**_SEED_BASE, "description": "NONE"},
                check_fails,
                id="uppercase_none_string_description",
            ),
            pytest.param(
                {**_SEED_BASE, "description": "\t"},
                check_fails,
                id="tab_only_description",
            ),
        ],
    )
    def test_check_seed_description_populated(self, seed_overrides, check_fn):
        check_fn(
            "check_seed_description_populated",
            seed=seed_overrides,
        )

    @pytest.mark.parametrize(
        ("description", "min_description_length", "check_fn"),
        [
            pytest.param(
                "1234567890", 10, check_passes, id="description_at_min_length"
            ),
            pytest.param(
                "123456789", 10, check_fails, id="description_one_below_min_length"
            ),
        ],
    )
    def test_check_seed_description_populated_boundary(
        self, description, min_description_length, check_fn
    ):
        check_fn(
            "check_seed_description_populated",
            seed={**_SEED_BASE, "description": description},
            min_description_length=min_description_length,
        )


class TestCheckSeedHasMetaKeys:
    @pytest.mark.parametrize(
        ("keys", "seed_overrides"),
        [
            pytest.param(
                ["owner"],
                {**_SEED_BASE, "meta": {"owner": "Data Team"}},
                id="has_key",
            ),
            pytest.param(
                ["owner"],
                {**_SEED_BASE, "meta": {"maturity": "high", "owner": "Data Team"}},
                id="has_key_with_others",
            ),
            pytest.param(
                ["owner", {"team": ["name", "slack"]}],
                {
                    **_SEED_BASE,
                    "meta": {
                        "owner": "Data Team",
                        "team": {"name": "Analytics", "slack": "#analytics"},
                    },
                },
                id="has_nested_keys",
            ),
            pytest.param(
                ["owner"],
                {**_SEED_BASE, "meta": {"owner": None}},
                id="key_present_with_none_value",
            ),
            pytest.param(
                ["owner"],
                {**_SEED_BASE, "meta": {"owner": ""}},
                id="key_present_with_empty_string_value",
            ),
            pytest.param(
                ["owner", {"team": [{"lead": ["name", "email"]}]}],
                {
                    **_SEED_BASE,
                    "meta": {
                        "owner": "Data Team",
                        "team": {"lead": {"name": "Bob", "email": "bob@example.com"}},
                    },
                },
                id="has_three_level_nested_keys",
            ),
        ],
    )
    def test_passes(self, keys, seed_overrides):
        check_passes("check_seed_has_meta_keys", keys=keys, seed=seed_overrides)

    @pytest.mark.parametrize(
        ("keys", "seed_overrides"),
        [
            pytest.param(
                ["owner"],
                {**_SEED_BASE, "meta": {}},
                id="empty_meta",
            ),
            pytest.param(
                ["owner"],
                {**_SEED_BASE, "meta": {"maturity": "high"}},
                id="missing_key",
            ),
            pytest.param(
                ["owner", {"team": ["name", "slack"]}],
                {
                    **_SEED_BASE,
                    "meta": {"owner": "Data Team", "team": {"name": "Analytics"}},
                },
                id="missing_nested_key",
            ),
            pytest.param(
                ["Owner"],
                {**_SEED_BASE, "meta": {"owner": "Data Team"}},
                id="key_case_mismatch",
            ),
            pytest.param(
                ["owner", {"team": [{"lead": ["name", "email"]}]}],
                {
                    **_SEED_BASE,
                    "meta": {
                        "owner": "Data Team",
                        "team": {"lead": {"name": "Bob"}},
                    },
                },
                id="missing_three_level_nested_key",
            ),
        ],
    )
    def test_fails(self, keys, seed_overrides):
        check_fails("check_seed_has_meta_keys", keys=keys, seed=seed_overrides)


_UNIT_TEST_FOR_SEED = {
    "depends_on": {"nodes": ["seed.package_name.raw_customers"]},
    "expect": {"format": "dict", "rows": [{"id": 1}]},
    "fqn": ["package_name", "raw_customers", "unit_test_1"],
    "given": [{"input": "ref(input_1)", "format": "csv"}],
    "model": "raw_customers",
    "name": "unit_test_1",
    "original_file_path": "seeds/_seeds.yml",
    "resource_type": "unit_test",
    "package_name": "package_name",
    "path": "_seeds.yml",
    "unique_id": "unit_test.package_name.raw_customers.unit_test_1",
}

_SEED_FOR_UNIT_TEST = {
    "alias": "raw_customers",
    "columns": {},
    "fqn": ["package_name", "raw_customers"],
    "name": "raw_customers",
    "original_file_path": "seeds/raw_customers.csv",
    "path": "raw_customers.csv",
    "unique_id": "seed.package_name.raw_customers",
}


_DBT_18_MANIFEST_METADATA = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
        "dbt_version": "1.8.0",
        "project_name": "dbt_bouncer_test_project",
        "adapter_type": "postgres",
    },
}


class TestCheckSeedHasUnitTests:
    @pytest.mark.parametrize(
        ("min_number_of_unit_tests", "check_fn"),
        [
            pytest.param(1, check_passes, id="has_unit_test"),
            pytest.param(2, check_fails, id="not_enough_unit_tests"),
        ],
    )
    def test_check_seed_has_unit_tests(self, min_number_of_unit_tests, check_fn):
        check_fn(
            "check_seed_has_unit_tests",
            seed=_SEED_FOR_UNIT_TEST,
            min_number_of_unit_tests=min_number_of_unit_tests,
            ctx_unit_tests=[_UNIT_TEST_FOR_SEED],
            ctx_manifest_obj=_DBT_18_MANIFEST_METADATA,
        )

    def test_check_seed_has_unit_tests_zero_unit_tests(self):
        check_fails(
            "check_seed_has_unit_tests",
            seed=_SEED_FOR_UNIT_TEST,
            min_number_of_unit_tests=1,
            ctx_manifest_obj=_DBT_18_MANIFEST_METADATA,
        )

    def test_check_seed_has_unit_tests_depends_on_different_seed(self):
        check_fails(
            "check_seed_has_unit_tests",
            seed=_SEED_FOR_UNIT_TEST,
            min_number_of_unit_tests=1,
            ctx_unit_tests=[
                {
                    **_UNIT_TEST_FOR_SEED,
                    "depends_on": {"nodes": ["seed.package_name.other_seed"]},
                }
            ],
            ctx_manifest_obj=_DBT_18_MANIFEST_METADATA,
        )

    def test_check_seed_has_unit_tests_empty_depends_on_nodes(self):
        check_fails(
            "check_seed_has_unit_tests",
            seed=_SEED_FOR_UNIT_TEST,
            min_number_of_unit_tests=1,
            ctx_unit_tests=[
                {
                    **_UNIT_TEST_FOR_SEED,
                    "depends_on": {"nodes": []},
                }
            ],
            ctx_manifest_obj=_DBT_18_MANIFEST_METADATA,
        )

    def test_check_seed_has_unit_tests_dbt_version_below_1_8_0(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            check_passes(
                "check_seed_has_unit_tests",
                seed=_SEED_FOR_UNIT_TEST,
                min_number_of_unit_tests=5,
                ctx_manifest_obj={"metadata": {"dbt_version": "1.7.0"}},
            )
        assert "1.8.0" in caplog.text


class TestCheckSeedNames:
    @pytest.mark.parametrize(
        ("seed_overrides", "seed_name_pattern", "check_fn"),
        [
            pytest.param(
                {
                    "alias": "raw_customers",
                    "columns": {},
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.sql",
                    "path": "raw_customers.sql",
                    "unique_id": "seed.package_name.raw_customers",
                },
                "^raw_",
                check_passes,
                id="seed_name_matches_pattern",
            ),
            pytest.param(
                {
                    "alias": "raw_customers",
                    "columns": {},
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.sql",
                    "path": "raw_customers.sql",
                    "unique_id": "seed.package_name.raw_customers",
                },
                "^seed_",
                check_fails,
                id="seed_name_does_not_match_pattern",
            ),
            pytest.param(
                {
                    "alias": "Raw_Customers",
                    "columns": {},
                    "fqn": ["package_name", "Raw_Customers"],
                    "name": "Raw_Customers",
                    "original_file_path": "seeds/Raw_Customers.sql",
                    "path": "Raw_Customers.sql",
                    "unique_id": "seed.package_name.Raw_Customers",
                },
                "^raw_",
                check_fails,
                id="seed_name_case_mismatch",
            ),
        ],
    )
    def test_check_seed_names(self, seed_overrides, seed_name_pattern, check_fn):
        check_fn(
            "check_seed_names",
            seed=seed_overrides,
            seed_name_pattern=seed_name_pattern,
        )

    def test_check_seed_names_invalid_regex(self):
        with pytest.raises(re.error):
            _run_check(
                "check_seed_names",
                seed={"name": "raw_customers"},
                seed_name_pattern="(unclosed",
            )
