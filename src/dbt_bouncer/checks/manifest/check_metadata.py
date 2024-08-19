import re
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.parsers import DbtBouncerManifest
from dbt_bouncer.utils import get_check_inputs


class CheckProjectName(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: Optional[int] = Field(
        default=None, description="Index to uniquely identify the check, calculated at runtime."
    )
    name: Literal["check_project_name"]
    project_name_pattern: str = Field(description="Regexp the project name must match.")


def check_project_name(manifest_obj: DbtBouncerManifest, request, check_config=None):
    """
    Enforce that the name of the dbt project matches a supplied regex. Generally used to enforce that project names conform to something like  `company_<DOMAIN>`.
    """

    check_config = get_check_inputs(check_config=check_config, request=request)["check_config"]

    assert (
        re.compile(check_config["project_name_pattern"].strip()).match(
            manifest_obj.manifest.metadata.project_name
        )
        is not None
    ), f"Project name (`{manifest_obj.manifest.metadata.project_name}`) does not conform to the supplied regex `({check_config['project_name_pattern'].strip()})`."
