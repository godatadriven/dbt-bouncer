import pytest

from dbt_bouncer.testing import check_fails, check_passes

_MODEL_2_STAGING = {
    "access": "public",
    "alias": "model_2",
    "fqn": ["package_name", "model_2"],
    "name": "model_2",
    "original_file_path": "models/staging/model_2.sql",
    "path": "model_2.sql",
    "unique_id": "model.package_name.model_2",
}

_UNIT_TEST_FOR_MODEL_2 = {
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
    "given": [{"input": "ref(input_1)", "format": "csv"}],
    "model": "model_2",
    "name": "unit_test_1",
    "original_file_path": "models/staging/crm/_crm__source.yml",
    "resource_type": "unit_test",
    "package_name": "package_name",
    "path": "staging/crm/_crm__source.yml",
    "unique_id": "unit_test.package_name.model_2.unit_test_1",
}


@pytest.mark.parametrize(
    (
        "description",
        "include",
        "min_unit_test_coverage_pct",
        "ctx_models",
        "ctx_unit_tests",
        "check_fn",
    ),
    [
        pytest.param(
            "The first check",
            "^models/staging",
            100,
            [_MODEL_2_STAGING],
            [_UNIT_TEST_FOR_MODEL_2],
            check_passes,
            id="100pct_coverage_met",
        ),
        pytest.param(
            None,
            "^models/staging",
            75,
            [
                {
                    "access": "public",
                    "alias": "model_1",
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "models/marts/model_1.sql",
                    "path": "model_1.sql",
                    "unique_id": "model.package_name.model_1",
                },
                _MODEL_2_STAGING,
            ],
            [_UNIT_TEST_FOR_MODEL_2],
            check_passes,
            id="75pct_coverage_met_non_matching_model_excluded",
        ),
        pytest.param(
            "The third check",
            "^models/staging",
            75,
            [
                {
                    "access": "public",
                    "alias": "model_1",
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "models/staging/model_1.sql",
                    "path": "model_1.sql",
                    "unique_id": "model.package_name.model_1",
                },
                _MODEL_2_STAGING,
            ],
            [_UNIT_TEST_FOR_MODEL_2],
            check_fails,
            id="75pct_coverage_not_met",
        ),
    ],
)
def test_check_unit_test_coverage(
    description,
    include,
    min_unit_test_coverage_pct,
    ctx_models,
    ctx_unit_tests,
    check_fn,
):
    kwargs = {
        "include": include,
        "min_unit_test_coverage_pct": min_unit_test_coverage_pct,
        "ctx_models": ctx_models,
        "ctx_unit_tests": ctx_unit_tests,
    }
    if description is not None:
        kwargs["description"] = description
    check_fn("check_unit_test_coverage", **kwargs)


@pytest.mark.parametrize(
    ("permitted_formats", "unit_test_overrides", "check_fn"),
    [
        pytest.param(
            ["csv", "dict", "sql"],
            {
                "expect": {"format": "dict", "rows": [{"id": 1}]},
                "given": [{"input": "ref(input_1)", "format": "csv"}],
            },
            check_passes,
            id="expect_format_permitted",
        ),
        pytest.param(
            ["csv", "sql"],
            {
                "expect": {"format": "dict", "rows": [{"id": 1}]},
                "given": [{"input": "ref(input_1)", "format": "csv"}],
            },
            check_fails,
            id="expect_format_not_permitted",
        ),
    ],
)
def test_check_unit_test_expect_format(
    permitted_formats, unit_test_overrides, check_fn
):
    check_fn(
        "check_unit_test_expect_format",
        permitted_formats=permitted_formats,
        unit_test=unit_test_overrides,
    )


@pytest.mark.parametrize(
    ("permitted_formats", "unit_test_overrides", "check_fn"),
    [
        pytest.param(
            ["csv", "dict", "sql"],
            {
                "given": [
                    {"input": "ref(input_1)", "format": "csv"},
                    {"input": "ref(input_2)", "format": "dict"},
                    {"input": "ref(input_3)", "format": "sql"},
                ],
                "expect": {"rows": [{"id": 1}]},
            },
            check_passes,
            id="all_given_formats_permitted",
        ),
        pytest.param(
            ["csv", "dict"],
            {
                "given": [
                    {"input": "ref(input_1)", "format": "csv"},
                    {"input": "ref(input_2)", "format": "dict"},
                    {"input": "ref(input_3)", "format": "sql"},
                ],
                "expect": {"rows": [{"id": 1}]},
            },
            check_fails,
            id="given_format_not_permitted",
        ),
    ],
)
def test_check_unit_test_given_formats(
    permitted_formats, unit_test_overrides, check_fn
):
    check_fn(
        "check_unit_test_given_formats",
        permitted_formats=permitted_formats,
        unit_test=unit_test_overrides,
    )
