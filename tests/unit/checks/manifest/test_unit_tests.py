import warnings
from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import UnitTests

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)

    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,  # noqa: F401
    )

import warnings

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import Nodes4
from dbt_bouncer.artifact_parsers.parsers_manifest import (
    DbtBouncerManifest,  # noqa: F401
)
from dbt_bouncer.checks.manifest.check_unit_tests import (
    CheckUnitTestCoverage,
    CheckUnitTestExpectFormats,
    CheckUnitTestGivenFormats,
)

CheckUnitTestCoverage.model_rebuild()
CheckUnitTestExpectFormats.model_rebuild()
CheckUnitTestGivenFormats.model_rebuild()


@pytest.mark.parametrize(
    (
        "include",
        "manifest_obj",
        "min_unit_test_coverage_pct",
        "models",
        "unit_tests",
        "expectation",
    ),
    [
        (
            "^models/staging",
            "manifest_obj",
            100,
            [
                Nodes4(
                    **{
                        "access": "public",
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "models/staging/model_2.sql",
                        "package_name": "package_name",
                        "path": "model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    },
                ),
            ],
            [
                UnitTests(
                    **{
                        "depends_on": {
                            "macros": [],
                            "nodes": ["model.package_name.model_2"],
                        },
                        "expect": {"format": "dict", "rows": [{"id": 1}]},
                        "fqn": [
                            "package_name",
                            "staging",
                            "crm",
                            "model_1",
                            "unit_test_1",
                        ],
                        "given": [
                            {"input": "ref(input_1)", "format": "csv"},
                        ],
                        "model": "model_2",
                        "name": "unit_test_1",
                        "original_file_path": "models/staging/crm/_crm__source.yml",
                        "resource_type": "unit_test",
                        "package_name": "package_name",
                        "path": "staging/crm/_crm__source.yml",
                        "unique_id": "unit_test.package_name.model_2.unit_test_1",
                    },
                )
            ],
            does_not_raise(),
        ),
        (
            "^models/staging",
            "manifest_obj",
            75,
            [
                Nodes4(
                    **{
                        "access": "public",
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "models/marts/model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    },
                ),
                Nodes4(
                    **{
                        "access": "public",
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "models/staging/model_2.sql",
                        "package_name": "package_name",
                        "path": "model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    },
                ),
            ],
            [
                UnitTests(
                    **{
                        "depends_on": {
                            "macros": [],
                            "nodes": ["model.package_name.model_2"],
                        },
                        "expect": {"format": "dict", "rows": [{"id": 1}]},
                        "fqn": [
                            "package_name",
                            "staging",
                            "crm",
                            "model_1",
                            "unit_test_1",
                        ],
                        "given": [
                            {"input": "ref(input_1)", "format": "csv"},
                        ],
                        "model": "model_2",
                        "name": "unit_test_1",
                        "original_file_path": "models/staging/crm/_crm__source.yml",
                        "resource_type": "unit_test",
                        "package_name": "package_name",
                        "path": "staging/crm/_crm__source.yml",
                        "unique_id": "unit_test.package_name.model_2.unit_test_1",
                    },
                )
            ],
            does_not_raise(),
        ),
        (
            "^models/staging",
            "manifest_obj",
            75,
            [
                Nodes4(
                    **{
                        "access": "public",
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "models/staging/model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    },
                ),
                Nodes4(
                    **{
                        "access": "public",
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "models/staging/model_2.sql",
                        "package_name": "package_name",
                        "path": "model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    },
                ),
            ],
            [
                UnitTests(
                    **{
                        "depends_on": {
                            "macros": [],
                            "nodes": ["model.package_name.model_2"],
                        },
                        "expect": {"format": "dict", "rows": [{"id": 1}]},
                        "fqn": [
                            "package_name",
                            "staging",
                            "crm",
                            "model_1",
                            "unit_test_1",
                        ],
                        "given": [
                            {"input": "ref(input_1)", "format": "csv"},
                        ],
                        "model": "model_2",
                        "name": "unit_test_1",
                        "original_file_path": "models/staging/crm/_crm__source.yml",
                        "resource_type": "unit_test",
                        "package_name": "package_name",
                        "path": "staging/crm/_crm__source.yml",
                        "unique_id": "unit_test.package_name.model_2.unit_test_1",
                    },
                )
            ],
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_unit_test_coverage(
    include,
    manifest_obj,
    min_unit_test_coverage_pct,
    models,
    unit_tests,
    expectation,
):
    with expectation:
        CheckUnitTestCoverage(
            include=include,
            manifest_obj=manifest_obj,
            min_unit_test_coverage_pct=min_unit_test_coverage_pct,
            models=models,
            name="check_unit_test_coverage",
            unit_tests=unit_tests,
        ).execute()


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
