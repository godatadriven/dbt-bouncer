from dbt_bouncer.check_decorator import check, fail


@check("check_source_columns_are_all_documented", iterate_over="catalog_source")
def check_source_columns_are_all_documented(catalog_source, ctx):
    """All columns in a source should be included in the source's properties file."""
    source = next(s for s in ctx.sources if s.unique_id == catalog_source.unique_id)
    undocumented_columns = [
        v.name
        for _, v in catalog_source.columns.items()
        if v.name not in (source.columns or {})
    ]
    if undocumented_columns:
        fail(
            f"`{catalog_source.unique_id}` has columns that are not included in the sources properties file: {undocumented_columns}"
        )
