# mypy: disable-error-code="union-attr"

from typing import Literal, Union

import pytest
from _pytest.fixtures import TopRequest
from pydantic import Field

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.parsers import DbtBouncerResult
from dbt_bouncer.utils import bouncer_check


class CheckRunResultsMaxGigabytesBilled(BaseCheck):
    max_gigabytes_billed: float = Field(
        description="The maximum gigagbytes billed allowed for a node."
    )
    name: Literal["check_run_results_max_gigabytes_billed"]


@pytest.mark.iterate_over_run_results
@bouncer_check
def check_run_results_max_gigabytes_billed(
    request: TopRequest,
    max_gigabytes_billed: Union[float, None] = None,
    run_result: Union[DbtBouncerResult, None] = None,
    **kwargs,
) -> None:
    """
    Each result can have a maximum number of gigabytes billed. Note that this only works for the `dbt-bigquery` adapter.
    """

    try:
        gigabytes_billed = run_result.adapter_response["bytes_billed"] / (1000**3)
    except KeyError:
        raise RuntimeError(
            "`bytes_billed` not found in adapter response. Are you using the `dbt-bigquery` adapter?"
        )

    assert (
        gigabytes_billed < max_gigabytes_billed
    ), f"`{run_result.unique_id.split('.')[-2]}` results in ({gigabytes_billed} billed bytes, this is greater than permitted ({max_gigabytes_billed})."


class CheckRunResultsMaxExecutionTime(BaseCheck):
    max_execution_time: float = Field(
        description="The maximum execution time (seconds) allowed for a node."
    )
    name: Literal["check_run_results_max_execution_time"]


@pytest.mark.iterate_over_run_results
@bouncer_check
def check_run_results_max_execution_time(
    request: TopRequest,
    max_execution_time: Union[float, None] = None,
    run_result: Union[DbtBouncerResult, None] = None,
    **kwargs,
) -> None:
    """
    Each result can take a maximum duration (seconds).
    """

    assert (
        run_result.execution_time <= max_execution_time
    ), f"`{run_result.unique_id.split('.')[-1]}` has an execution time ({run_result.execution_time} greater than permitted ({max_execution_time}s)."
