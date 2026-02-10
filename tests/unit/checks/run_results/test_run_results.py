from contextlib import nullcontext as does_not_raise

import pytest
from pydantic import TypeAdapter

from dbt_bouncer.artifact_parsers.parsers_run_results import DbtBouncerRunResultBase
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.run_results.check_run_results import (
    CheckRunResultsMaxExecutionTime,
    CheckRunResultsMaxGigabytesBilled,
)


@pytest.fixture
def run_result(request):
    default_run_result = {
        "adapter_response": {"bytes_billed": 1},
        "execution_time": 1,
        "status": "success",
        "thread_id": "Thread-1",
        "timing": [],
        "unique_id": "model.package_name.model_1",
    }
    return TypeAdapter(DbtBouncerRunResultBase).validate_python(
        {**default_run_result, **getattr(request, "param", {})}
    )


_TEST_DATA_FOR_CHECK_RUN_RESULTS_MAX_GIGABYTES_BILLED = [
    pytest.param(
        10,
        {},
        does_not_raise(),
        id="within_limit",
    ),
    pytest.param(
        10,
        {
            "adapter_response": {"bytes_billed": 100000000000},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_limit",
    ),
]


@pytest.mark.parametrize(
    ("max_gigabytes_billed", "run_result", "expectation"),
    _TEST_DATA_FOR_CHECK_RUN_RESULTS_MAX_GIGABYTES_BILLED,
    indirect=["run_result"],
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


_TEST_DATA_FOR_CHECK_RUN_RESULTS_MAX_EXECUTION_TIME = [
    pytest.param(
        10,
        {},
        does_not_raise(),
        id="within_limit",
    ),
    pytest.param(
        10,
        {
            "execution_time": 10,
        },
        does_not_raise(),
        id="at_limit",
    ),
    pytest.param(
        10,
        {
            "execution_time": 100,
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_limit",
    ),
]


@pytest.mark.parametrize(
    ("max_execution_time_seconds", "run_result", "expectation"),
    _TEST_DATA_FOR_CHECK_RUN_RESULTS_MAX_EXECUTION_TIME,
    indirect=["run_result"],
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
