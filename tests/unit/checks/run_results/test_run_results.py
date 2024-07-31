from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.run_results.check_run_results import (
    check_run_results_max_execution_time,
)


@pytest.mark.parametrize(
    "check_config, run_result, expectation",
    [
        (
            {
                "max_execution_time": 10,
            },
            {
                "execution_time": 1,
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "max_execution_time": 10,
            },
            {
                "execution_time": 10,
                "unique_id": "model.package_name.model_2",
            },
            does_not_raise(),
        ),
        (
            {
                "max_execution_time": 10,
            },
            {
                "execution_time": 100,
                "unique_id": "model.package_name.model_3",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_run_results_max_execution_time(check_config, run_result, expectation):
    with expectation:
        check_run_results_max_execution_time(
            check_config=check_config, run_result=run_result, request=None
        )
