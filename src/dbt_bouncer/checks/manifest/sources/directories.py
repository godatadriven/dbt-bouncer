"""Checks related to source directory and file locations."""

from pathlib import Path

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import clean_path_str


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
