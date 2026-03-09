"""Checks related to source directory and file locations."""

from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerSourceBase,
    )

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import clean_path_str


class CheckSourcePropertyFileLocation(BaseCheck):
    """Source properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

    Receives:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

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

    name: Literal["check_source_property_file_location"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If property file location is incorrect.

        """
        source = self._require_source()
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

        if not properties_yml_name.startswith("_"):
            raise DbtBouncerFailedCheckError(
                f"The properties file for `{source.source_name}.{source.name}` (`{properties_yml_name}`) does not start with an underscore."
            )
        if expected_substring not in properties_yml_name:
            raise DbtBouncerFailedCheckError(
                f"The properties file for `{source.source_name}.{source.name}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substring}`)."
            )
        if not properties_yml_name.endswith("__sources.yml"):
            raise DbtBouncerFailedCheckError(
                f"The properties file for `{source.source_name}.{source.name}` (`{properties_yml_name}`) does not end with `__sources.yml`."
            )
