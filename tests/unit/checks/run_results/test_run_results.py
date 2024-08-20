import warnings
from contextlib import nullcontext as does_not_raise

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.run_results.run_results_v6 import Result

from dbt_bouncer.checks.run_results.check_run_results import (
    check_run_results_max_execution_time,
    check_run_results_max_gigabytes_billed,
)


@pytest.mark.parametrize(
    "max_gigabytes_billed, run_result, expectation",
    [
        (
            10,
            Result(
                **{
                    "adapter_response": {"bytes_billed": 1},
                    "execution_time": 1,
                    "status": "success",
                    "thread_id": "Thread-1",
                    "timing": [],
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            10,
            Result(
                **{
                    "adapter_response": {"bytes_billed": 100000000000},
                    "execution_time": 1,
                    "status": "success",
                    "thread_id": "Thread-1",
                    "timing": [],
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_run_results_max_gigabytes_billed(max_gigabytes_billed, run_result, expectation):
    with expectation:
        check_run_results_max_gigabytes_billed(
            max_gigabytes_billed=max_gigabytes_billed, run_result=run_result, request=None
        )


@pytest.mark.parametrize(
    "max_execution_time, run_result, expectation",
    [
        (
            10,
            Result(
                **{
                    "adapter_response": {"bytes_billed": 1},
                    "execution_time": 1,
                    "status": "success",
                    "thread_id": "Thread-1",
                    "timing": [],
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            10,
            Result(
                **{
                    "adapter_response": {"bytes_billed": 1},
                    "execution_time": 10,
                    "status": "success",
                    "thread_id": "Thread-1",
                    "timing": [],
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            10,
            Result(
                **{
                    "adapter_response": {"bytes_billed": 1},
                    "execution_time": 100,
                    "status": "success",
                    "thread_id": "Thread-1",
                    "timing": [],
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_run_results_max_execution_time(max_execution_time, run_result, expectation):
    with expectation:
        check_run_results_max_execution_time(
            max_execution_time=max_execution_time, run_result=run_result, request=None
        )
