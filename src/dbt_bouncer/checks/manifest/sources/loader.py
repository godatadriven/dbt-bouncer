"""Checks related to source loaders."""

from dbt_bouncer.check_decorator import check, fail


@check("check_source_loader_populated", iterate_over="source")
def check_source_loader_populated(source):
    """Sources must have a populated loader."""
    if source.loader == "":
        fail(f"`{source.source_name}.{source.name}` does not have a populated loader.")
