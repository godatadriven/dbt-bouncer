import pytest

from dbt_bouncer.testing import check_fails, check_passes


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
    ],
)
def test_check_seed_column_names(seed_overrides, seed_column_name_pattern, check_fn):
    check_fn(
        "check_seed_column_names",
        seed=seed_overrides,
        seed_column_name_pattern=seed_column_name_pattern,
    )


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
    ],
)
def test_check_seed_columns_have_types(seed_overrides, check_fn):
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


@pytest.mark.parametrize(
    ("seed_overrides", "check_fn"),
    [
        pytest.param(
            {
                **_SEED_BASE,
                "description": "Description that is more than 4 characters.",
            },
            check_passes,
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
        ),
        pytest.param(
            {**_SEED_BASE, "description": ""},
            check_fails,
        ),
        pytest.param(
            {**_SEED_BASE, "description": " "},
            check_fails,
        ),
        pytest.param(
            {
                **_SEED_BASE,
                "description": """
                            """,
            },
            check_fails,
        ),
        pytest.param(
            {**_SEED_BASE, "description": "-"},
            check_fails,
        ),
        pytest.param(
            {**_SEED_BASE, "description": "null"},
            check_fails,
        ),
    ],
)
def test_check_seed_description_populated(seed_overrides, check_fn):
    check_fn(
        "check_seed_description_populated",
        seed=seed_overrides,
    )


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


@pytest.mark.parametrize(
    ("min_number_of_unit_tests", "check_fn"),
    [
        pytest.param(1, check_passes, id="has_unit_test"),
        pytest.param(2, check_fails, id="not_enough_unit_tests"),
    ],
)
def test_check_seed_has_unit_tests(min_number_of_unit_tests, check_fn):
    check_fn(
        "check_seed_has_unit_tests",
        seed=_SEED_FOR_UNIT_TEST,
        min_number_of_unit_tests=min_number_of_unit_tests,
        ctx_unit_tests=[_UNIT_TEST_FOR_SEED],
        ctx_manifest_obj={
            "metadata": {
                "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
                "dbt_version": "1.8.0",
                "project_name": "dbt_bouncer_test_project",
                "adapter_type": "postgres",
            },
        },
    )


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
        ),
    ],
)
def test_check_seed_names(seed_overrides, seed_name_pattern, check_fn):
    check_fn(
        "check_seed_names",
        seed=seed_overrides,
        seed_name_pattern=seed_name_pattern,
    )
