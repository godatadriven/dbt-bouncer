"""Source checks that use the dbt Information Schema."""

from dbt_bouncer.check_framework.decorator import check, fail


@check(code="IS009")
def check_source_columns_are_used(source, ctx):
    """Every column declared on a source must be used by at least one downstream model.

    !!! info "Rationale"

        Source YAML is often generated once and never pruned, so it documents, tests and sometimes loads columns that no model reads. Unused columns cost ingestion and test time, and they suggest to readers that the data is in use. dbt's column-level lineage shows which source columns models actually read, so this check can list the declared columns that nothing uses.

    !!! note

        This check requires the dbt Information Schema (dbt 2.0+, `--generate-info-schema`). A column counts as used when any lineage edge reads it, including joins and filters. When a downstream model has no column-level lineage (dbt's static analysis could not analyse it), the source is not checked, because its columns cannot be proven unused. Ephemeral downstream models are the exception: dbt records their lineage on the models that select from them. Sources without downstream models are not checked: use `check_source_not_orphaned` for those.

    Receives:
        source (SourceNode): The SourceNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the source path. Source paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the source path. Only source paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        info_schema_checks:
            - name: check_source_columns_are_used
        ```

    """
    # dbt inlines ephemeral models, so their lineage is recorded on their consumers.
    children = [
        child
        for child in ctx.children_by_unique_id.get(source.unique_id, [])
        if not (child.config and child.config.materialized == "ephemeral")
    ]
    lineage = ctx.info_schema.lineage_by_child
    if not children or any(child.unique_id not in lineage for child in children):
        return
    used = ctx.info_schema.lineage_parent_columns.get(source.unique_id, set())
    unused = sorted(
        name for name in (source.columns or {}) if name.casefold() not in used
    )
    if unused:
        fail(
            f"`{source.unique_id}` declares columns that no model uses: {', '.join(f'`{c}`' for c in unused)}."
        )
