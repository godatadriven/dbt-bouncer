from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.tests.test_models import test_populated_model_description


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
def test_test_populated_model_description(model, expectation):
    with expectation:
        test_populated_model_description(model=model, request=None)
