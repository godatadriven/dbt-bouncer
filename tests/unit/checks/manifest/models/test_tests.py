from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.check_context import CheckContext
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.models.tests import (
    CheckModelHasUniqueTest,
    CheckModelHasUnitTests,
    CheckModelTestCoverage,
)

_TEST_DATA_FOR_CHECK_MODEL_HAS_UNIQUE_TEST = [
    pytest.param(
        ["expect_compound_columns_to_be_unique", "unique"],
        {},
        [{}],
        does_not_raise(),
        id="has_unique_test",
    ),
    pytest.param(
        ["my_custom_test", "unique"],
        {},
        [
            {"test_metadata": {"name": "my_custom_test"}},
        ],
        does_not_raise(),
        id="has_custom_unique_test",
    ),
    pytest.param(
        ["unique"],
        {},
        [
            {"test_metadata": {"name": "expect_compound_columns_to_be_unique"}},
        ],
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_unique_test_strict",
    ),
    pytest.param(
        ["expect_compound_columns_to_be_unique", "unique"],
        {},
        [],
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_unique_test",
    ),
]


@pytest.mark.parametrize(
    ("accepted_uniqueness_tests", "model", "tests", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_UNIQUE_TEST,
    indirect=["model", "tests"],
)
def test_check_model_has_unique_test(
    accepted_uniqueness_tests,
    model,
    tests,
    expectation,
):
    with expectation:
        check = CheckModelHasUniqueTest(
            accepted_uniqueness_tests=accepted_uniqueness_tests,
            model=model,
            name="check_model_has_unique_test",
        )
        check._ctx = CheckContext(tests=tests)
        check.execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_UNIT_TESTS = [
    pytest.param(
        "manifest_obj",
        1,
        {},
        [{}],
        does_not_raise(),
        id="has_unit_test",
    ),
    pytest.param(
        "manifest_obj",
        2,
        {},
        [{}],
        pytest.raises(DbtBouncerFailedCheckError),
        id="not_enough_unit_tests",
    ),
]


@pytest.mark.parametrize(
    ("manifest_obj", "min_number_of_unit_tests", "model", "unit_tests", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_UNIT_TESTS,
    indirect=["manifest_obj", "model", "unit_tests"],
)
def test_check_model_has_unit_tests(
    manifest_obj,
    min_number_of_unit_tests,
    model,
    unit_tests,
    expectation,
):
    with expectation:
        check = CheckModelHasUnitTests(
            min_number_of_unit_tests=min_number_of_unit_tests,
            model=model,
            name="check_model_has_unit_tests",
        )
        check._ctx = CheckContext(
            manifest_obj=manifest_obj,
            unit_tests=unit_tests,
        )
        check.execute()


_TEST_DATA_FOR_CHECK_MODEL_TEST_COVERAGE = [
    pytest.param(
        100,
        [{}],
        [
            {
                "alias": "not_null_model_1_unique",
                "attached_node": "model.package_name.model_1",
                "checksum": {"name": "none", "checksum": ""},
                "column_name": "col_1",
                "depends_on": {
                    "nodes": [
                        "model.package_name.model_1",
                    ],
                },
                "fqn": [
                    "package_name",
                    "marts",
                    "finance",
                    "not_null_model_1_unique",
                ],
                "name": "not_null_model_1_unique",
                "original_file_path": "models/marts/finance/_finance__models.yml",
                "package_name": "package_name",
                "path": "not_null_model_1_unique.sql",
                "resource_type": "test",
                "schema": "main",
                "test_metadata": {
                    "name": "expect_compound_columns_to_be_unique",
                },
                "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
            },
        ],
        does_not_raise(),
        id="100_percent_coverage",
    ),
    pytest.param(
        50,
        [
            {
                "alias": "model_1",
                "fqn": ["package_name", "model_1"],
                "name": "model_1",
                "original_file_path": "model_1.sql",
                "path": "staging/finance/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            {
                "alias": "model_2",
                "fqn": ["package_name", "model_2"],
                "name": "model_2",
                "original_file_path": "model_2.sql",
                "path": "staging/finance/model_2.sql",
                "unique_id": "model.package_name.model_2",
            },
        ],
        [
            {
                "alias": "not_null_model_1_unique",
                "attached_node": "model.package_name.model_1",
                "depends_on": {
                    "nodes": [
                        "model.package_name.model_1",
                    ],
                },
                "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
            },
        ],
        does_not_raise(),
        id="50_percent_coverage",
    ),
    pytest.param(
        100,
        [
            {
                "alias": "model_1",
                "fqn": ["package_name", "model_1"],
                "name": "model_1",
                "original_file_path": "model_1.sql",
                "path": "staging/finance/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            {
                "alias": "model_2",
                "fqn": ["package_name", "model_2"],
                "name": "model_2",
                "original_file_path": "model_2.sql",
                "path": "staging/finance/model_2.sql",
                "unique_id": "model.package_name.model_2",
            },
        ],
        [
            {
                "alias": "not_null_model_1_unique",
                "attached_node": "model.package_name.model_1",
                "depends_on": {
                    "nodes": [
                        "model.package_name.model_2",
                    ],
                },
                "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
            },
        ],
        pytest.raises(DbtBouncerFailedCheckError),
        id="less_than_100_percent_coverage",
    ),
]


@pytest.mark.parametrize(
    ("min_model_test_coverage_pct", "models", "tests", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_TEST_COVERAGE,
    indirect=["models", "tests"],
)
def test_check_model_test_coverage(
    min_model_test_coverage_pct,
    models,
    tests,
    expectation,
):
    with expectation:
        check = CheckModelTestCoverage(
            min_model_test_coverage_pct=min_model_test_coverage_pct,
            name="check_model_test_coverage",
        )
        check._ctx = CheckContext(models=models, tests=tests)
        check.execute()
