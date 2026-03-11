from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.check_context import CheckContext
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.models.description import (
    CheckModelDescriptionContainsRegexPattern,
    CheckModelDescriptionPopulated,
    CheckModelDocumentationCoverage,
    CheckModelDocumentedInSameDirectory,
)

_TEST_DATA_FOR_CHECK_MODEL_DESCRIPTION_CONTAINS_REGEX_PATTERN = [
    pytest.param(
        {
            "description": "Description that contains the pattern to match.",
        },
        ".*pattern to match.*",
        does_not_raise(),
        id="contains_pattern_single_line",
    ),
    pytest.param(
        {
            "description": """A
                        multiline
                        description
                        with the pattern to match.
                        """,
        },
        ".*pattern to match.*",
        does_not_raise(),
        id="contains_pattern_multiline",
    ),
    pytest.param(
        {
            "description": "",
        },
        ".*pattern to match.*",
        pytest.raises(DbtBouncerFailedCheckError),
        id="empty_description",
    ),
    pytest.param(
        {
            "description": " ",
        },
        ".*pattern to match.*",
        pytest.raises(DbtBouncerFailedCheckError),
        id="whitespace_description",
    ),
    pytest.param(
        {
            "description": """
                        """,
        },
        ".*pattern to match.*",
        pytest.raises(DbtBouncerFailedCheckError),
        id="multiline_whitespace_description",
    ),
    pytest.param(
        {
            "description": "Description with a pattern that does not match.",
        },
        ".*pattern to match.*",
        pytest.raises(DbtBouncerFailedCheckError),
        id="does_not_contain_pattern",
    ),
    pytest.param(
        {
            "description": """Description with
                    the
                    pattern to match.""",
        },
        ".*pattern to match.*",
        does_not_raise(),
        id="contains_pattern_multiline_split",
    ),
]


@pytest.mark.parametrize(
    ("model", "pattern", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DESCRIPTION_CONTAINS_REGEX_PATTERN,
    indirect=["model"],
)
def test_check_model_description_contains_regex_pattern(model, pattern, expectation):
    with expectation:
        CheckModelDescriptionContainsRegexPattern(
            model=model,
            name="check_model_description_contains_regex_pattern",
            regexp_pattern=pattern,
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_DESCRIPTION_POPULATED = [
    pytest.param(
        {
            "description": "Description that is more than 4 characters.",
        },
        does_not_raise(),
        id="populated_description",
    ),
    pytest.param(
        {
            "description": """A
                        multiline
                        description
                        """,
        },
        does_not_raise(),
        id="multiline_description",
    ),
    pytest.param(
        {
            "description": "",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="empty_description",
    ),
    pytest.param(
        {
            "description": " ",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="whitespace_description",
    ),
    pytest.param(
        {
            "description": """
                        """,
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="multiline_whitespace_description",
    ),
    pytest.param(
        {
            "description": "-",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="too_short_description",
    ),
    pytest.param(
        {
            "description": "null",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="null_description",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DESCRIPTION_POPULATED,
    indirect=["model"],
)
def test_check_model_description_populated(model, expectation):
    with expectation:
        CheckModelDescriptionPopulated(
            model=model, name="check_model_description_populated"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_DOCUMENTATION_COVERAGE = [
    pytest.param(
        100,
        [
            {
                "alias": "model_2",
                "description": "Model 2 description",
                "fqn": ["package_name", "model_2"],
                "name": "model_2",
                "original_file_path": "model_2.sql",
                "path": "model_2.sql",
                "unique_id": "model.package_name.model_2",
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
                "description": "Model 1 description",
                "fqn": ["package_name", "model_1"],
                "name": "model_1",
                "original_file_path": "model_1.sql",
                "path": "model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            {
                "alias": "model_2",
                "description": "",
                "fqn": ["package_name", "model_2"],
                "name": "model_2",
                "original_file_path": "model_2.sql",
                "path": "model_2.sql",
                "unique_id": "model.package_name.model_2",
            },
        ],
        does_not_raise(),
        id="50_percent_coverage",
    ),
    pytest.param(
        100,
        [
            {
                "alias": "model_2",
                "description": "",
                "fqn": ["package_name", "model_2"],
                "name": "model_2",
                "original_file_path": "model_2.sql",
                "path": "model_2.sql",
                "unique_id": "model.package_name.model_2",
            },
        ],
        pytest.raises(DbtBouncerFailedCheckError),
        id="0_percent_coverage",
    ),
]


@pytest.mark.parametrize(
    ("min_model_documentation_coverage_pct", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DOCUMENTATION_COVERAGE,
    indirect=["models"],
)
def test_check_model_documentation_coverage(
    min_model_documentation_coverage_pct,
    models,
    expectation,
):
    with expectation:
        check = CheckModelDocumentationCoverage(
            min_model_documentation_coverage_pct=min_model_documentation_coverage_pct,
            name="check_model_documentation_coverage",
        )
        check._ctx = CheckContext(models=models)
        check.execute()


_TEST_DATA_FOR_CHECK_MODEL_DOCUMENTED_IN_SAME_DIRECTORY = [
    pytest.param(
        {
            "alias": "model_1",
            "fqn": ["package_name", "model_1"],
            "name": "model_1",
            "original_file_path": "models/staging/model_1.sql",
            "patch_path": "package_name://models/staging/_schema.yml",
            "path": "staging/model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        does_not_raise(),
        id="same_directory",
    ),
    pytest.param(
        {
            "alias": "model_1",
            "fqn": ["package_name", "model_1"],
            "name": "model_1",
            "original_file_path": "models/staging/finance/model_1.sql",
            "patch_path": "package_name://models/staging/_schema.yml",
            "path": "staging/finance/model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="different_directory",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DOCUMENTED_IN_SAME_DIRECTORY,
    indirect=["model"],
)
def test_check_model_documented_in_same_directory(model, expectation):
    with expectation:
        CheckModelDocumentedInSameDirectory(
            model=model, name="check_model_documented_in_same_directory"
        ).execute()
