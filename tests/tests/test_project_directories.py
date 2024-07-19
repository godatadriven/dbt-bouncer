from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.tests.test_project_directories import test_top_level_directories


@pytest.mark.parametrize(
    "models, expectation",
    [
        (
            [
                {
                    "path": "staging/model_1.sql",
                    "unique_id": "model.package_name.model_1",
                }
            ],
            does_not_raise(),
        ),
        (
            [
                {
                    "path": "intermediate/model_1.sql",
                    "unique_id": "model.package_name.model_1",
                }
            ],
            does_not_raise(),
        ),
        (
            [
                {
                    "path": "marts/model_1.sql",
                    "unique_id": "model.package_name.model_1",
                }
            ],
            does_not_raise(),
        ),
        (
            [
                {
                    "path": "model_1.sql",
                    "unique_id": "model.package_name.model_1",
                }
            ],
            pytest.raises(AssertionError),
        ),
        (
            [
                {
                    "path": "aggregation/model_1.sql",
                    "unique_id": "model.package_name.model_1",
                }
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_test_top_level_directories(models, expectation):
    with expectation:
        test_top_level_directories(models=models)
