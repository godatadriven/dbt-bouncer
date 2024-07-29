import re
from typing import Literal

from pydantic import Field

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.utils import get_check_inputs


class CheckProjectName(BaseCheck):
    name: Literal["check_project_name"]
    project_name_pattern: str = Field(description="Regexp the project name must match.")


def check_project_name(manifest_obj, request, check_config=None):
    """
    Enforce that the name of the dbt project matches a supplied regex. Generally used to enforce that project names conform to something like  `company_<DOMAIN>`.
    """

    check_config = get_check_inputs(check_config=check_config, request=request)["check_config"]

    assert (
        re.compile(check_config["project_name_pattern"].strip()).match(
            manifest_obj.metadata.project_name
        )
        is not None
    ), f"Project name (`{manifest_obj.metadata.project_name}`) does not conform to the supplied regex `({check_config['project_name_pattern'].strip()})`."