"""Checks related to source directory and file locations."""

from pathlib import Path

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import clean_path_str


@check
def check_source_property_file_location(source):
    """Source properties files must follow the guidance provided by dbt."""
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
