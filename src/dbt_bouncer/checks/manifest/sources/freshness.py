"""Checks related to source freshness."""

from dbt_bouncer.check_decorator import check, fail


@check("check_source_freshness_populated", iterate_over="source")
def check_source_freshness_populated(source):
    """Sources must have a populated freshness."""
    display = f"{source.source_name}.{source.name}"
    error_msg = f"`{display}` does not have a populated freshness."

    if source.freshness is None:
        fail(error_msg)

    has_error_after = (
        source.freshness.error_after
        and source.freshness.error_after.count is not None
        and source.freshness.error_after.period is not None
    )
    has_warn_after = (
        source.freshness.warn_after
        and source.freshness.warn_after.count is not None
        and source.freshness.warn_after.period is not None
    )

    if not (has_error_after or has_warn_after):
        fail(error_msg)
