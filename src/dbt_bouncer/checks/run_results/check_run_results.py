from typing import TYPE_CHECKING, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_run_results import DbtBouncerRunResultBase

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class CheckRunResultsMaxExecutionTime(BaseCheck):
    """Each result can take a maximum duration (seconds).

    Parameters:
        max_execution_time_seconds (float): The maximum execution time (seconds) allowed for a node.

    Receives:
        run_result (DbtBouncerRunResult): The DbtBouncerRunResult object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the resource path. Resource paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the resource path. Only resource paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

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
    run_result: "DbtBouncerRunResultBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If execution time is greater than permitted.

        """
        if self.run_result is None:
            raise DbtBouncerFailedCheckError("self.run_result is None")

        if self.run_result.execution_time > self.max_execution_time_seconds:
            raise DbtBouncerFailedCheckError(
                f"`{self.run_result.unique_id.split('.')[-1]}` has an execution time ({self.run_result.execution_time} greater than permitted ({self.max_execution_time_seconds}s)."
            )


class CheckRunResultsMaxGigabytesBilled(BaseCheck):
    """Each result can have a maximum number of gigabytes billed.

    !!! note

        Note that this check only works for the `dbt-bigquery` adapter.

    Parameters:
        max_gigabytes_billed (float): The maximum number of gigabytes billed.
        run_result (DbtBouncerRunResult): The DbtBouncerRunResult object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the resource path. Resource paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the resource path. Only resource paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Raises: # noqa:DOC502
        KeyError: If the `dbt-bigquery` adapter is not used.

    Example(s):
        ```yaml
        run_results_checks:
            - name: check_run_results_max_gigabytes_billed
              max_gigabytes_billed: 100
              exclude: ^seeds
        ```

    """

    max_gigabytes_billed: float
    name: Literal["check_run_results_max_gigabytes_billed"]
    run_result: "DbtBouncerRunResultBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If gigabytes billed is greater than permitted.
            RuntimeError: If running with adapter other than `dbt-bigquery`.

        """
        if self.run_result is None:
            raise DbtBouncerFailedCheckError("self.run_result is None")

        try:
            gigabytes_billed = self.run_result.adapter_response["bytes_billed"] / (
                1000**3
            )
        except KeyError as e:
            raise RuntimeError(
                "`bytes_billed` not found in adapter response. Are you using the `dbt-bigquery` adapter?",
            ) from e

        if gigabytes_billed > self.max_gigabytes_billed:
            raise DbtBouncerFailedCheckError(
                f"`{self.run_result.unique_id.split('.')[-2]}` results in ({gigabytes_billed} billed bytes, this is greater than permitted ({self.max_gigabytes_billed})."
            )
