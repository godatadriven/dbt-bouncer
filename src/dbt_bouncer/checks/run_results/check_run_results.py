"""Checks related to run result metrics."""

from dbt_bouncer.check_decorator import check, fail


@check
def check_run_results_max_execution_time(
    run_result, *, max_execution_time_seconds: float
):
    """Each result can take a maximum duration (seconds).

    Parameters:
        max_execution_time_seconds (float): The maximum execution time (seconds) allowed for a node.

    Receives:
        run_result (RunResultEntry): The RunResultEntry object to check.

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
    if run_result.execution_time > max_execution_time_seconds:
        fail(
            f"`{run_result.unique_id.split('.')[-1]}` has an execution time ({run_result.execution_time} greater than permitted ({max_execution_time_seconds}s)."
        )


@check
def check_run_results_max_gigabytes_billed(run_result, *, max_gigabytes_billed: float):
    """Each result can have a maximum number of gigabytes billed.

    !!! note

        Note that this check only works for the `dbt-bigquery` adapter.

    Parameters:
        max_gigabytes_billed (float): The maximum number of gigabytes billed.
        run_result (RunResultEntry): The RunResultEntry object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the resource path. Resource paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the resource path. Only resource paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Raises:
        RuntimeError: If the `dbt-bigquery` adapter is not used.

    Example(s):
        ```yaml
        run_results_checks:
            - name: check_run_results_max_gigabytes_billed
              max_gigabytes_billed: 100
              exclude: ^seeds
        ```

    """
    try:
        gigabytes_billed = run_result.adapter_response["bytes_billed"] / (1000**3)
    except KeyError as e:
        raise RuntimeError(
            "`bytes_billed` not found in adapter response. Are you using the `dbt-bigquery` adapter?"
        ) from e

    if gigabytes_billed > max_gigabytes_billed:
        fail(
            f"`{run_result.unique_id.split('.')[-2]}` results in ({gigabytes_billed} billed bytes, this is greater than permitted ({max_gigabytes_billed})."
        )
