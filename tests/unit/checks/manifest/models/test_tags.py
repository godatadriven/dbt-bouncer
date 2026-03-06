from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.models.tags import (
    CheckModelHasTags,
)

_TEST_DATA_FOR_CHECK_MODEL_HAS_TAGS = [
    pytest.param(
        {
            "tags": ["tag_1"],
        },
        ["tag_1"],
        "all",
        does_not_raise(),
        id="has_all_tags",
    ),
    pytest.param(
        {
            "tags": [],
        },
        ["tag_1"],
        "all",
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_tag",
    ),
    pytest.param(
        {
            "tags": ["tag_1", "tag_2"],
        },
        ["tag_1", "tag_2"],
        "all",
        does_not_raise(),
        id="has_all_multiple_tags",
    ),
    pytest.param(
        {
            "tags": ["tag_1"],
        },
        ["tag_1", "tag_2"],
        "all",
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_one_tag",
    ),
    pytest.param(
        {
            "tags": ["tag_1"],
        },
        ["tag_1", "tag_2"],
        "any",
        does_not_raise(),
        id="has_any_tag",
    ),
    pytest.param(
        {
            "tags": ["tag_3", "tag_4"],
        },
        ["tag_1", "tag_2"],
        "any",
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_any_tag",
    ),
    pytest.param(
        {
            "tags": ["tag_1", "tag_3"],
        },
        ["tag_1", "tag_2"],
        "one",
        does_not_raise(),
        id="has_one_tag",
    ),
    pytest.param(
        {
            "tags": ["tag_1", "tag_2"],
        },
        ["tag_1", "tag_2"],
        "one",
        pytest.raises(DbtBouncerFailedCheckError),
        id="has_more_than_one_tag",
    ),
]


@pytest.mark.parametrize(
    ("model", "tags", "criteria", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_TAGS,
    indirect=["model"],
)
def test_check_model_has_tags(model, tags, criteria, expectation):
    with expectation:
        CheckModelHasTags(
            model=model,
            name="check_model_has_tags",
            tags=tags,
            criteria=criteria,
        ).execute()
