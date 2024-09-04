import warnings
from contextlib import nullcontext as does_not_raise

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import UnitTests

from dbt_bouncer.checks.manifest.check_unit_tests import (
    CheckUnitTestExpectFormats,
    CheckUnitTestGivenFormats,
)
from dbt_bouncer.parsers import DbtBouncerManifest  # noqa: F401

CheckUnitTestExpectFormats.model_rebuild()
CheckUnitTestGivenFormats.model_rebuild()


@pytest.mark.parametrize(
    ("manifest_obj", "permitted_formats", "unit_test", "expectation"),
    [
        (
            "manifest_obj",
            ["csv", "dict", "sql"],
            UnitTests(
                **{
                    "expect": {"format": "dict", "rows": [{"id": 1}]},
                    "fqn": ["package_name", "staging", "crm", "model_1", "unit_test_1"],
                    "given": [
                        {"input": "ref(input_1)", "format": "csv"},
                    ],
                    "model": "model_1",
                    "name": "unit_test_1",
                    "original_file_path": "models/staging/crm/_crm__source.yml",
                    "resource_type": "unit_test",
                    "package_name": "package_name",
                    "path": "staging/crm/_crm__source.yml",
                    "unique_id": "unit_test.package_name.model_1.unit_test_1",
                },
            ),
            does_not_raise(),
        ),
        (
            "manifest_obj",
            ["csv", "sql"],
            UnitTests(
                **{
                    "expect": {"format": "dict", "rows": [{"id": 1}]},
                    "fqn": ["package_name", "staging", "crm", "model_1", "unit_test_1"],
                    "given": [{"input": "ref(input_1)", "format": "csv"}],
                    "model": "model_1",
                    "name": "unit_test_1",
                    "original_file_path": "models/staging/crm/_crm__source.yml",
                    "resource_type": "unit_test",
                    "package_name": "package_name",
                    "path": "staging/crm/_crm__source.yml",
                    "unique_id": "unit_test.package_name.model_1.unit_test_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_unit_test_expect_format(
    manifest_obj,
    permitted_formats,
    unit_test,
    expectation,
):
    with expectation:
        CheckUnitTestExpectFormats(
            manifest_obj=manifest_obj,
            name="check_unit_test_expect_format",
            permitted_formats=permitted_formats,
            unit_test=unit_test,
        ).execute()


@pytest.mark.parametrize(
    ("manifest_obj", "permitted_formats", "unit_test", "expectation"),
    [
        (
            "manifest_obj",
            ["csv", "dict", "sql"],
            UnitTests(
                **{
                    "model": "model_1",
                    "given": [
                        {"input": "ref(input_1)", "format": "csv"},
                        {"input": "ref(input_2)", "format": "dict"},
                        {"input": "ref(input_3)", "format": "sql"},
                    ],
                    "expect": {"rows": [{"id": 1}]},
                    "fqn": ["package_name", "staging", "crm", "model_1", "unit_test_1"],
                    "name": "unit_test_1",
                    "original_file_path": "models/staging/crm/_crm__source.yml",
                    "resource_type": "unit_test",
                    "package_name": "package_name",
                    "path": "staging/crm/_crm__source.yml",
                    "unique_id": "unit_test.package_name.model_1.unit_test_1",
                },
            ),
            does_not_raise(),
        ),
        (
            "manifest_obj",
            ["csv", "dict"],
            UnitTests(
                **{
                    "model": "model_1",
                    "given": [
                        {"input": "ref(input_1)", "format": "csv"},
                        {"input": "ref(input_2)", "format": "dict"},
                        {"input": "ref(input_3)", "format": "sql"},
                    ],
                    "expect": {"rows": [{"id": 1}]},
                    "fqn": ["package_name", "staging", "crm", "model_1", "unit_test_1"],
                    "name": "unit_test_1",
                    "original_file_path": "models/staging/crm/_crm__source.yml",
                    "resource_type": "unit_test",
                    "package_name": "package_name",
                    "path": "staging/crm/_crm__source.yml",
                    "unique_id": "unit_test.package_name.model_1.unit_test_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_unit_test_given_formats(
    manifest_obj,
    permitted_formats,
    unit_test,
    expectation,
):
    with expectation:
        CheckUnitTestGivenFormats(
            manifest_obj=manifest_obj,
            name="check_unit_test_given_formats",
            permitted_formats=permitted_formats,
            unit_test=unit_test,
        ).execute()
