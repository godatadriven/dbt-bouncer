"""Checks related to source freshness."""

from dbt_bouncer.check_decorator import check, fail


@check
def check_source_freshness_populated(source):
    """Sources must have a populated freshness.

    !!! info "Rationale"

        Source freshness configuration enables `dbt source freshness` to detect when upstream data stops arriving. Without it, stale data silently propagates through downstream models, and dashboards display outdated numbers without any warning. Requiring freshness definitions ensures that every source has an explicit SLA, enabling proactive alerting before business users notice a problem.

    Receives:
        source (SourceNode): The SourceNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_freshness_populated
        ```

    """
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
