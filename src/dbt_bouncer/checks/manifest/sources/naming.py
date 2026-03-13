"""Checks related to source naming conventions."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern


@check
def check_source_names(source, *, source_name_pattern: str):
    """Sources must have a name that matches the supplied regex."""
    compiled = compile_pattern(source_name_pattern.strip())
    display = f"{source.source_name}.{source.name}"
    if compiled.match(str(source.name)) is None:
        fail(
            f"`{display}` does not match the supplied regex `{source_name_pattern.strip()}`."
        )
