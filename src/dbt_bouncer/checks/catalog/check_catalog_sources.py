from dbt_bouncer.check_framework.decorator import check, fail


@check(code="CA004")
def check_source_columns_are_all_documented(
    catalog_source, ctx, *, case_sensitive: bool = True
):
    """All columns in a source should be included in the source's properties file, i.e. `.yml` file.

    !!! info "Rationale"

        Source tables are the entry point for raw data into a dbt project. When a column exists in the database but is absent from the source properties file, it cannot have a description, a freshness check, or a data test applied to it. Over time, undocumented columns accumulate silently, making it harder to understand what data is available and creating blind spots in data quality monitoring. This check enforces full column coverage so that every raw field is explicitly acknowledged and can be tested or documented.

    Parameters:
        case_sensitive (bool): Whether the column names are case sensitive or not. Necessary for adapters like `dbt-snowflake` where the column in `catalog.json` is uppercase but the column in `manifest.json` can be lowercase. Defaults to `false` for `dbt-snowflake`, otherwise `true`.

    Receives:
        catalog_source (CatalogNodeEntry): The CatalogNodeEntry object to check.
        manifest_obj (ManifestObject): The ManifestObject object parsed from `manifest.json`.
        sources (list[SourceNode]): List of SourceNode objects parsed from `catalog.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Source paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Only source paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_source_columns_are_all_documented
        ```

    """
    source = next(s for s in ctx.sources if s.unique_id == catalog_source.unique_id)

    if ctx.manifest_obj.manifest.metadata.adapter_type in ["snowflake"]:
        case_sensitive = False

    source_columns = source.columns or {}
    if case_sensitive:
        undocumented_columns = [
            v.name
            for _, v in catalog_source.columns.items()
            if v.name not in source_columns
        ]
    else:
        source_columns_lower = {c.lower() for c in source_columns}
        undocumented_columns = [
            v.name
            for _, v in catalog_source.columns.items()
            if v.name.lower() not in source_columns_lower
        ]

    if undocumented_columns:
        fail(
            f"`{catalog_source.unique_id}` has columns that are not included in the sources properties file: {undocumented_columns}"
        )
