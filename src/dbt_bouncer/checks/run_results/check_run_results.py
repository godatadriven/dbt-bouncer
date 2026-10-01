"""Checks related to run result metrics."""

from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_framework.decorator import check, fail


@check(code="RR001")
def check_run_results_max_execution_time(
    run_result, *, max_execution_time_seconds: Annotated[float, Field(gt=0)]
):
    """Each result can take a maximum duration (seconds).

    !!! info "Rationale"

        Model execution times can creep up gradually as data volumes grow or queries become more complex. Without an explicit threshold, a model that once ran in 10 seconds can silently grow to 10 minutes, eventually causing pipeline timeouts or SLA breaches. This check acts as a performance guardrail, catching regressions early so teams can investigate and optimise before they impact production schedules.

    Parameters:
        max_execution_time_seconds (float): The maximum execution time (seconds) allowed for a node.

    Receives:
        run_result (RunResultEntry): The RunResultEntry object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the resource path. Resource paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the resource path. Only resource paths that match any pattern will be checked.
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


@check(code="RR002")
def check_run_results_max_gigabytes_billed(
    run_result, *, max_gigabytes_billed: Annotated[float, Field(gt=0)]
):
    """Each result can have a maximum number of gigabytes billed.

    !!! info "Rationale"

        BigQuery charges are based on the volume of data scanned per query, so a poorly optimised model or an accidental full-table scan can generate unexpectedly large bills. Without an explicit cap, a single expensive run can blow through a project's monthly data budget before anyone notices. This check provides a cost guardrail that fails a CI run or pipeline job if any model scans more data than permitted, prompting investigation and optimisation before the bill arrives.

    !!! note

        Note that this check only works for the `dbt-bigquery` adapter. Nodes that ran no query (e.g. skipped or errored nodes, which have an empty adapter response) have nothing billed and pass.

    Parameters:
        max_gigabytes_billed (float): The maximum number of gigabytes billed.

    Receives:
        run_result (RunResultEntry): The RunResultEntry object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the resource path. Resource paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the resource path. Only resource paths that match any pattern will be checked.
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
    adapter_response = run_result.adapter_response
    if not adapter_response:
        # Skipped and errored nodes ran no query, so dbt records an empty
        # adapter response: nothing was billed.
        return

    try:
        gigabytes_billed = adapter_response["bytes_billed"] / (1000**3)
    except KeyError as e:
        raise RuntimeError(
            "`bytes_billed` not found in adapter response. Are you using the `dbt-bigquery` adapter?"
        ) from e

    if gigabytes_billed > max_gigabytes_billed:
        fail(
            f"`{run_result.unique_id.split('.')[-2]}` results in ({gigabytes_billed} billed bytes, this is greater than permitted ({max_gigabytes_billed})."
        )
