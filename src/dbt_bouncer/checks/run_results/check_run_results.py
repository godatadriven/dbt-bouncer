# mypy: disable-error-code="union-attr"

from typing import TYPE_CHECKING, Literal

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.parsers import DbtBouncerRunResult


class CheckRunResultsMaxGigabytesBilled(BaseCheck):
    max_gigabytes_billed: float
    name: Literal["check_run_results_max_gigabytes_billed"]


def check_run_results_max_gigabytes_billed(
    max_gigabytes_billed: float,
    run_result: "DbtBouncerRunResult",
    **kwargs,
) -> None:
    """Each result can have a maximum number of gigabytes billed.

    !!! note

        Note that this check only works for the `dbt-bigquery` adapter.

    Parameters:
        max_gigabytes_billed (float): The maximum number of gigabytes billed.
        run_result (DbtBouncerRunResult): The DbtBouncerRunResult object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the resource path. Resource paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the resource path. Only resource paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Raises: # noqa:DOC502
        KeyError: If the `dbt-bigquery` adapter is not used.

    Example(s):
        ```yaml
        run_results_checks:
            - name: check_run_results_max_gigabytes_billed
              max_gigabytes_billed: 100
        ```

    """
    try:
        gigabytes_billed = run_result.adapter_response["bytes_billed"] / (1000**3)
    except KeyError as e:
        raise RuntimeError(  # noqa: DOC501
            "`bytes_billed` not found in adapter response. Are you using the `dbt-bigquery` adapter?",
        ) from e

    assert (
        gigabytes_billed < max_gigabytes_billed
    ), f"`{run_result.unique_id.split('.')[-2]}` results in ({gigabytes_billed} billed bytes, this is greater than permitted ({max_gigabytes_billed})."


class CheckRunResultsMaxExecutionTime(BaseCheck):
    max_execution_time_seconds: float
    name: Literal["check_run_results_max_execution_time"]


def check_run_results_max_execution_time(
    max_execution_time_seconds: float,
    run_result: "DbtBouncerRunResult",
    **kwargs,
) -> None:
    """Each result can take a maximum duration (seconds).

    Parameters:
        max_execution_time_seconds (float): The maximum execution time (seconds) allowed for a node.
        run_result (DbtBouncerRunResult): The DbtBouncerRunResult object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the resource path. Resource paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the resource path. Only resource paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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
