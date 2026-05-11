"""Checks related to source directory and file locations."""

from pathlib import Path

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import clean_path_str, compile_pattern


@check
def check_source_property_file_location(source):
    """Source properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

    !!! info "Rationale"

        dbt's official project structure guide recommends placing source properties files in the staging layer alongside the models that consume them, with a predictable naming convention (e.g. `_<layer>__sources.yml`). Following this convention makes it immediately obvious where to find a source's configuration, prevents properties files from being scattered across arbitrary directories, and ensures that new team members can navigate the project without guesswork. This check codifies the convention so it is enforced automatically rather than relied upon by convention alone.

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
            - name: check_source_property_file_location
        ```

    """
    original_path = Path(clean_path_str(source.original_file_path))

    if (
        len(original_path.parts) > 2
        and original_path.parts[0] == "models"
        and original_path.parts[1] == "staging"
    ):
        subdir_parts = original_path.parent.parts[2:]
    else:
        subdir_parts = original_path.parent.parts

    expected_substring = "_" + "_".join(subdir_parts) if subdir_parts else ""
    properties_yml_name = original_path.name
    display = f"{source.source_name}.{source.name}"

    if not properties_yml_name.startswith("_"):
        fail(
            f"The properties file for `{display}` (`{properties_yml_name}`) does not start with an underscore."
        )
    if expected_substring not in properties_yml_name:
        fail(
            f"The properties file for `{display}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substring}`)."
        )
    if not properties_yml_name.endswith("__sources.yml"):
        fail(
            f"The properties file for `{display}` (`{properties_yml_name}`) does not end with `__sources.yml`."
        )


@check
def check_source_file_name(source, *, file_name_pattern: str):
    r"""Sources must have a file name that matches the supplied regex.

    !!! info "Rationale"

        Consistent file naming conventions (e.g. `_stripe__sources.yml`) make it easy to identify a source's purpose at a glance. Enforcing naming patterns in CI prevents deviations that accumulate over time and make the project harder to navigate.

    Parameters:
        file_name_pattern (str): Regexp the file name must match. Please account for the `.yml` extension.

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
            - name: check_source_file_name
              description: Sources must have a file name that starts with an underscore and ends with `__sources.yml`.
              file_name_pattern: ^_.*__sources\.yml$
        ```

    """
    compiled = compile_pattern(file_name_pattern.strip())
    file_name = Path(clean_path_str(source.original_file_path)).name
    display = f"{source.source_name}.{source.name}"
    if compiled.match(file_name) is None:
        fail(
            f"`{display}` is in a file (`{file_name}`) that does not match the supplied regex `{file_name_pattern.strip()}`."
        )
