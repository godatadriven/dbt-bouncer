"""Checks related to run result metrics."""

from dbt_bouncer.check_decorator import check, fail


@check(
    "check_run_results_max_execution_time",
    iterate_over="run_result",
    params={"max_execution_time_seconds": float},
)
def check_run_results_max_execution_time(
    run_result, ctx, *, max_execution_time_seconds: float
):
    """Each result can take a maximum duration (seconds)."""
    if run_result.execution_time > max_execution_time_seconds:
        fail(
            f"`{run_result.unique_id.split('.')[-1]}` has an execution time ({run_result.execution_time} greater than permitted ({max_execution_time_seconds}s)."
        )


@check(
    "check_run_results_max_gigabytes_billed",
    iterate_over="run_result",
    params={"max_gigabytes_billed": float},
)
def check_run_results_max_gigabytes_billed(
    run_result, ctx, *, max_gigabytes_billed: float
):
    """Each result can have a maximum number of gigabytes billed.

    Raises:
        RuntimeError: If the `dbt-bigquery` adapter is not used.

    """
    try:
        gigabytes_billed = run_result.adapter_response["bytes_billed"] / (1000**3)
    except KeyError as e:
        raise RuntimeError(
            "`bytes_billed` not found in adapter response. Are you using the `dbt-bigquery` adapter?",
        ) from e

    if gigabytes_billed > max_gigabytes_billed:
        fail(
            f"`{run_result.unique_id.split('.')[-2]}` results in ({gigabytes_billed} billed bytes, this is greater than permitted ({max_gigabytes_billed})."
        )
