from pathlib import Path
from typing import List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated

from dbt_bouncer.logger import logger


class BaseCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include: Optional[str] = Field(
        default=None, description="Regexp to match which paths to include."
    )


class CheckModelNames(BaseCheck):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    name: Literal["check_model_names"]
    model_name_pattern: str = Field(description="Regexp the model name must match.")


class CheckPopulatedModelDescription(BaseCheck):
    name: Literal["check_populated_model_description"]


class CheckProjectName(BaseCheck):
    name: Literal["check_project_name"]
    project_name_pattern: str = Field(description="Regexp the project name must match.")


class CheckTopLevelDirectories(BaseCheck):
    name: Literal["check_top_level_directories"]


CheckConfigs = Annotated[
    Union[
        CheckModelNames, CheckPopulatedModelDescription, CheckProjectName, CheckTopLevelDirectories
    ],
    Field(discriminator="name"),
]


class DbtBouncerConfigFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    checks: List[CheckConfigs]
    dbt_artifacts_dir: Optional[Path] = Field(alias="dbt-artifacts-dir", default=Path("./target"))


def validate_config_file(file: Path) -> DbtBouncerConfigFile:
    logger.info("Validating config file...")
    with Path.open(file, "r") as f:
        definitions = yaml.safe_load(f)

    return DbtBouncerConfigFile(**definitions)
