# mypy: disable-error-code="union-attr"

import re
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.parsers import DbtBouncerManifest


class CheckProjectName(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: Optional[int] = Field(
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    name: Literal["check_project_name"]
    project_name_pattern: str


def check_project_name(
    manifest_obj: DbtBouncerManifest,
    project_name_pattern: str,
    **kwargs,
) -> None:
    """Enforce that the name of the dbt project matches a supplied regex. Generally used to enforce that project names conform to something like  `company_<DOMAIN>`.

    Parameters:
        manifest_obj (DbtBouncerManifest): The manifest object.
        project_name_pattern (str): Regex pattern to match the project name.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_project_name
              project_name_pattern: ^awesome_company_
        ```

    """
    assert (
        re.compile(project_name_pattern.strip()).match(
            manifest_obj.manifest.metadata.project_name,
        )
        is not None
    ), f"Project name (`{manifest_obj.manifest.metadata.project_name}`) does not conform to the supplied regex `({project_name_pattern.strip()})`."
