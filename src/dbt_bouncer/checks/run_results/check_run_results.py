# mypy: disable-error-code="union-attr"

from typing import Literal

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.parsers import DbtBouncerRunResult


class CheckRunResultsMaxGigabytesBilled(BaseCheck):
    max_gigabytes_billed: float
    name: Literal["check_run_results_max_gigabytes_billed"]


def check_run_results_max_gigabytes_billed(
    max_gigabytes_billed: float,
    run_result: DbtBouncerRunResult,
    **kwargs,
) -> None:
    """
    Each result can have a maximum number of gigabytes billed.

    !!! note

        Note that this check only works for the `dbt-bigquery` adapter.

    Receives:
        exclude (Optional[str]): Regex pattern to match the resource path. Resource paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the resource path. Only resource paths that match the pattern will be checked.
        max_gigabytes_billed (float): The maximum gigabytes billed allowed for a node.
        run_result (DbtBouncerResult): The DbtBouncerResult object to check.

    Example(s):
        ```yaml
        run_results_checks:
            - name: check_run_results_max_gigabytes_billed
              max_gigabytes_billed: 100
        ```
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
    max_execution_time_seconds: float
    name: Literal["check_run_results_max_execution_time"]


def check_run_results_max_execution_time(
    max_execution_time_seconds: float,
    run_result: DbtBouncerRunResult,
    **kwargs,
) -> None:
    """
    Each result can take a maximum duration (seconds).

    Receives:
        exclude (Optional[str]): Regex pattern to match the resource path. Resource paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the resource path. Only resource paths that match the pattern will be checked.
        max_execution_time_seconds (float): The maximum execution time (seconds) allowed for a node.
        run_result (DbtBouncerResult): The DbtBouncerResult object to check.

    Example(s):
        ```yaml
        run_results_checks:
            - name: check_run_results_max_execution_time
              max_execution_time_seconds: 60
        ```
        ```yaml
        run_results_checks:
            - name: check_run_results_max_execution_time
              include: ^models/staging # Not a good idea, here for demonstration purposes only
              max_execution_time_seconds: 10
        ```
    """

    assert (
        run_result.execution_time <= max_execution_time_seconds
    ), f"`{run_result.unique_id.split('.')[-1]}` has an execution time ({run_result.execution_time} greater than permitted ({max_execution_time_seconds}s)."
