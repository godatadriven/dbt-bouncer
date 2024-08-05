from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.manifest.check_models import (
    check_model_access,
    check_model_code_does_not_contain_regexp_pattern,
    check_model_description_populated,
    check_model_has_unique_test,
    check_model_names,
)


@pytest.mark.parametrize(
    "check_config, model, expectation",
    [
        (
            {
                "access": "public",
            },
            {
                "access": "public",
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {"access": "public"},
            {
                "access": "protected",
                "unique_id": "model.package_name.mart_model_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_access(check_config, model, expectation):
    with expectation:
        check_model_access(check_config=check_config, model=model, request=None)


@pytest.mark.parametrize(
    "check_config, model, tests, expectation",
    [
        (
            {
                "accepted_uniqueness_tests": ["expect_compound_columns_to_be_unique", "unique"],
            },
            {
                "unique_id": "model.package_name.stg_model_1",
            },
            [
                {
                    "attached_node": "model.package_name.stg_model_1",
                    "test_metadata": {"name": "unique"},
                }
            ],
            does_not_raise(),
        ),
        (
            {
                "accepted_uniqueness_tests": ["my_custom_test", "unique"],
            },
            {
                "unique_id": "model.package_name.stg_model_2",
            },
            [
                {
                    "attached_node": "model.package_name.stg_model_2",
                    "test_metadata": {"name": "my_custom_test"},
                }
            ],
            does_not_raise(),
        ),
        (
            {
                "accepted_uniqueness_tests": ["unique"],
            },
            {
                "unique_id": "model.package_name.stg_model_3",
            },
            [
                {
                    "attached_node": "model.package_name.stg_model_3",
                    "test_metadata": {"name": "expect_compound_columns_to_be_unique"},
                }
            ],
            pytest.raises(AssertionError),
        ),
        (
            {
                "accepted_uniqueness_tests": ["expect_compound_columns_to_be_unique", "unique"],
            },
            {
                "unique_id": "model.package_name.stg_model_4",
            },
            [],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_has_unique_test(check_config, model, tests, expectation):
    with expectation:
        check_model_has_unique_test(
            check_config=check_config, model=model, tests=tests, request=None
        )


@pytest.mark.parametrize(
    "check_config, model, expectation",
    [
        (
            {"regexp_pattern": ".*[i][f][n][u][l][l].*"},
            {
                "raw_code": "select coalesce(a, b) from table",
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "regexp_pattern": ".*[i][f][n][u][l][l].*",
            },
            {
                "raw_code": "select ifnull(a, b) from table",
                "unique_id": "model.package_name.stg_model_2",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_code_does_not_contain_regexp_pattern(check_config, model, expectation):
    with expectation:
        check_model_code_does_not_contain_regexp_pattern(
            check_config=check_config, model=model, request=None
        )


@pytest.mark.parametrize(
    "check_config, model, expectation",
    [
        (
            {
                "model_name_pattern": "^stg_",
            },
            {
                "name": "stg_model_1",
                "path": "staging/stg_model_1.sql",
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "include": "^staging",
                "model_name_pattern": "^stg_",
            },
            {
                "name": "stg_model_2",
                "path": "staging/stg_model_2.sql",
                "unique_id": "model.package_name.stg_model_2",
            },
            does_not_raise(),
        ),
        (
            {
                "include": "^intermediate",
                "model_name_pattern": "^stg_",
            },
            {
                "name": "stg_model_3",
                "path": "staging/stg_model_3.sql",
                "unique_id": "model.package_name.stg_model_3",
            },
            does_not_raise(),
        ),
        (
            {
                "include": "^intermediate",
                "model_name_pattern": "^int_",
            },
            {
                "name": "int_model_1",
                "path": "intermediate/int_model_1.sql",
                "unique_id": "model.package_name.int_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "include": "^intermediate",
                "model_name_pattern": "^int_",
            },
            {
                "name": "model_1",
                "path": "intermediate/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "include": "^intermediate",
                "model_name_pattern": "^int_",
            },
            {
                "name": "model_int_2",
                "path": "intermediate/model_int_2.sql",
                "unique_id": "model.package_name.model_int_2",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_mode_names(check_config, model, expectation):
    with expectation:
        check_model_names(check_config=check_config, model=model, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            {
                "description": "Description that is more than 4 characters.",
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "description": """A
                        multiline
                        description
                        """,
                "unique_id": "model.package_name.model_2",
            },
            does_not_raise(),
        ),
        (
            {
                "description": "",
                "unique_id": "model.package_name.model_3",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": " ",
                "unique_id": "model.package_name.model_4",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": """
                        """,
                "unique_id": "model.package_name.model_5",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": "-",
                "unique_id": "model.package_name.model_6",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": "null",
                "unique_id": "model.package_name.model_7",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_description_populated(model, expectation):
    with expectation:
        check_model_description_populated(model=model, request=None)
