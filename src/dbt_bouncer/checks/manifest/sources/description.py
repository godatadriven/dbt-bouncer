"""Checks related to source descriptions."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import is_description_populated


@check("check_source_description_populated", iterate_over="source")
def check_source_description_populated(
    source, *, min_description_length: int | None = None
):
    """Sources must have a populated description."""
    desc = source.description or ""
    display = f"{source.source_name}.{source.name}"
    if not is_description_populated(desc, min_description_length or 4):
        fail(f"`{display}` does not have a populated description.")
