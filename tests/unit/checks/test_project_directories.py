from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.check_project_directories import check_top_level_directories


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            {
                "path": "staging/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "path": "intermediate/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "path": "marts/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "path": "model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "path": "aggregation/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_top_level_directories(model, expectation):
    with expectation:
        check_top_level_directories(model=model, request=None)
