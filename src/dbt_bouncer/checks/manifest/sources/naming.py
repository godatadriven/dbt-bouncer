"""Checks related to source naming conventions."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern


@check
def check_source_names(source, *, source_name_pattern: str):
    """Sources must have a name that matches the supplied regex.

    Parameters:
        source_name_pattern (str): Regexp the source name must match.

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
            - name: check_source_names
              source_name_pattern: >
                ^[a-z0-9_]*$
        ```

    """
    compiled = compile_pattern(source_name_pattern.strip())
    display = f"{source.source_name}.{source.name}"
    if compiled.match(str(source.name)) is None:
        fail(
            f"`{display}` does not match the supplied regex `{source_name_pattern.strip()}`."
        )
