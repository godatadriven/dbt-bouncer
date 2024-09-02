# mypy: disable-error-code="union-attr"

from typing import TYPE_CHECKING, Literal, Optional

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.parsers import DbtBouncerRunResultBase

import warnings

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)


class CheckRunResultsMaxGigabytesBilled(BaseCheck):
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

    max_gigabytes_billed: float
    name: Literal["check_run_results_max_gigabytes_billed"]
    run_result: Optional["DbtBouncerRunResultBase"] = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        try:
            gigabytes_billed = self.run_result.adapter_response["bytes_billed"] / (
                1000**3
            )
        except KeyError as e:
            raise RuntimeError(  # noqa: DOC501
                "`bytes_billed` not found in adapter response. Are you using the `dbt-bigquery` adapter?",
            ) from e

        assert (
            gigabytes_billed < self.max_gigabytes_billed
        ), f"`{self.run_result.unique_id.split('.')[-2]}` results in ({gigabytes_billed} billed bytes, this is greater than permitted ({self.max_gigabytes_billed})."


class CheckRunResultsMaxExecutionTime(BaseCheck):
    """Each result can take a maximum duration (seconds).

    Parameters:
        max_execution_time_seconds (float): The maximum execution time (seconds) allowed for a node.

    Receives:
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

    max_execution_time_seconds: float
    name: Literal["check_run_results_max_execution_time"]
    run_result: Optional["DbtBouncerRunResultBase"] = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        assert (
            self.run_result.execution_time <= self.max_execution_time_seconds
        ), f"`{self.run_result.unique_id.split('.')[-1]}` has an execution time ({self.run_result.execution_time} greater than permitted ({self.max_execution_time_seconds}s)."
