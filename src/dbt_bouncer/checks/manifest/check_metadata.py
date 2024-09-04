# mypy: disable-error-code="union-attr"

import re
from typing import TYPE_CHECKING, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from dbt_bouncer.parsers import DbtBouncerManifest


class CheckProjectName(BaseModel):
    """Enforce that the name of the dbt project matches a supplied regex. Generally used to enforce that project names conform to something like  `company_<DOMAIN>`.

    Parameters:
        project_name_pattern (str): Regex pattern to match the project name.

    Receives:
        manifest_obj (DbtBouncerManifest): The manifest object.

    Other Parameters:
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_project_name
              project_name_pattern: ^awesome_company_
        ```

    """

    model_config = ConfigDict(extra="forbid")

    index: Optional[int] = Field(
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    manifest_obj: "DbtBouncerManifest" = Field(default=None)
    name: Literal["check_project_name"]
    project_name_pattern: str
    severity: Optional[Literal["error", "warn"]] = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )

    def execute(self) -> None:
        """Execute the check."""
        assert (
            re.compile(self.project_name_pattern.strip()).match(
                self.manifest_obj.manifest.metadata.project_name,
            )
            is not None
        ), f"Project name (`{self.manifest_obj.manifest.metadata.project_name}`) does not conform to the supplied regex `({self.project_name_pattern.strip()})`."
