import warnings
from contextlib import nullcontext as does_not_raise

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)

from pydantic import TypeAdapter

from dbt_bouncer.artifact_parsers.parsers_run_results import DbtBouncerRunResultBase
from dbt_bouncer.checks.run_results.check_run_results import (
    CheckRunResultsMaxExecutionTime,
    CheckRunResultsMaxGigabytesBilled,
)

CheckRunResultsMaxGigabytesBilled.model_rebuild()
CheckRunResultsMaxExecutionTime.model_rebuild()


@pytest.mark.parametrize(
    ("max_gigabytes_billed", "run_result", "expectation"),
    [
        (
            10,
            TypeAdapter(DbtBouncerRunResultBase).validate_python(
                {
                    "adapter_response": {"bytes_billed": 1},
                    "execution_time": 1,
                    "status": "success",
                    "thread_id": "Thread-1",
                    "timing": [],
                    "unique_id": "model.package_name.model_1",
                },
            ),
            does_not_raise(),
        ),
        (
            10,
            TypeAdapter(DbtBouncerRunResultBase).validate_python(
                {
                    "adapter_response": {"bytes_billed": 100000000000},
                    "execution_time": 1,
                    "status": "success",
                    "thread_id": "Thread-1",
                    "timing": [],
                    "unique_id": "model.package_name.model_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_run_results_max_gigabytes_billed(
    max_gigabytes_billed,
    run_result,
    expectation,
):
    with expectation:
        CheckRunResultsMaxGigabytesBilled(
            max_gigabytes_billed=max_gigabytes_billed,
            name="check_run_results_max_gigabytes_billed",
            run_result=run_result,
        ).execute()


@pytest.mark.parametrize(
    ("max_execution_time_seconds", "run_result", "expectation"),
    [
        (
            10,
            TypeAdapter(DbtBouncerRunResultBase).validate_python(
                {
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
            TypeAdapter(DbtBouncerRunResultBase).validate_python(
                {
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
            TypeAdapter(DbtBouncerRunResultBase).validate_python(
                {
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
def test_check_run_results_max_execution_time(
    max_execution_time_seconds,
    run_result,
    expectation,
):
    with expectation:
        CheckRunResultsMaxExecutionTime(
            max_execution_time_seconds=max_execution_time_seconds,
            name="check_run_results_max_execution_time",
            run_result=run_result,
        ).execute()
