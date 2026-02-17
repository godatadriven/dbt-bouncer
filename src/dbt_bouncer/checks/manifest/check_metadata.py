import re
from typing import TYPE_CHECKING, Literal

from pydantic import ConfigDict, Field, PrivateAttr

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import compile_pattern

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import DbtBouncerManifest


class CheckProjectName(BaseCheck):
    """Enforce that the name of the dbt project matches a supplied regex. Generally used to enforce that project names conform to something like  `company_<DOMAIN>`.

    Parameters:
        project_name_pattern (str): Regex pattern to match the project name.

    Receives:
        manifest_obj (DbtBouncerManifest): The manifest object.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_project_name
              project_name_pattern: ^awesome_company_
        ```

    """

    model_config = ConfigDict(extra="forbid")

    description: str | None = Field(
        default=None,
        description="Description of what the check does and why it is implemented.",
    )
    index: int | None = Field(
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    name: Literal["check_project_name"]
    package_name: str | None = Field(default=None)
    project_name_pattern: str
    severity: Literal["error", "warn"] | None = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )

    _compiled_project_name_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_project_name_pattern = compile_pattern(
            self.project_name_pattern.strip()
        )

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If project name does not match regex.

        """
        if self.manifest_obj is None:
            raise DbtBouncerFailedCheckError("self.manifest_obj is None")

        package_name = (
            self.package_name or self.manifest_obj.manifest.metadata.project_name
        )
        if (
            self._compiled_project_name_pattern.match(
                str(package_name),
            )
            is None
        ):
            raise DbtBouncerFailedCheckError(
                f"Project name (`{package_name}`) does not conform to the supplied regex `({self.project_name_pattern.strip()})`."
            )
