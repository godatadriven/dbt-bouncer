from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.run_results.check_run_results import (
    check_run_results_max_execution_time,
    check_run_results_max_gigabytes_billed,
)
from dbt_bouncer.parsers import DbtBouncerResult


@pytest.mark.parametrize(
    "check_config, run_result, expectation",
    [
        (
            {
                "max_gigabytes_billed": 10,
            },
            DbtBouncerResult(
                **{
                    "path": "model_1.sql",
                    "result": {
                        "adapter_response": {"bytes_billed": 1},
                        "execution_time": 1,
                        "status": "success",
                        "thread_id": "Thread-1",
                        "timing": [],
                        "unique_id": "model.package_name.model_1",
                    },
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            {
                "max_gigabytes_billed": 10,
            },
            DbtBouncerResult(
                **{
                    "path": "model_1.sql",
                    "result": {
                        "adapter_response": {"bytes_billed": 100000000000},
                        "execution_time": 1,
                        "status": "success",
                        "thread_id": "Thread-1",
                        "timing": [],
                        "unique_id": "model.package_name.model_1",
                    },
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_run_results_max_gigabytes_billed(check_config, run_result, expectation):
    with expectation:
        check_run_results_max_gigabytes_billed(
            check_config=check_config, run_result=run_result, request=None
        )


@pytest.mark.parametrize(
    "check_config, run_result, expectation",
    [
        (
            {
                "max_execution_time": 10,
            },
            DbtBouncerResult(
                **{
                    "path": "model_1.sql",
                    "result": {
                        "adapter_response": {"bytes_billed": 1},
                        "execution_time": 1,
                        "status": "success",
                        "thread_id": "Thread-1",
                        "timing": [],
                        "unique_id": "model.package_name.model_1",
                    },
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            {
                "max_execution_time": 10,
            },
            DbtBouncerResult(
                **{
                    "path": "model_1.sql",
                    "result": {
                        "adapter_response": {"bytes_billed": 1},
                        "execution_time": 10,
                        "status": "success",
                        "thread_id": "Thread-1",
                        "timing": [],
                        "unique_id": "model.package_name.model_1",
                    },
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            {
                "max_execution_time": 10,
            },
            DbtBouncerResult(
                **{
                    "path": "model_1.sql",
                    "result": {
                        "adapter_response": {"bytes_billed": 1},
                        "execution_time": 100,
                        "status": "success",
                        "thread_id": "Thread-1",
                        "timing": [],
                        "unique_id": "model.package_name.model_1",
                    },
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_run_results_max_execution_time(check_config, run_result, expectation):
    with expectation:
        check_run_results_max_execution_time(
            check_config=check_config, run_result=run_result, request=None
        )
