from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated

from dbt_bouncer.logger import logger


class BaseCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include: Optional[str] = Field(
        default=None, description="Regexp to match which paths to include."
    )


class CheckMacroArgumentsDescriptionPopulated(BaseCheck):
    name: Literal["check_macro_arguments_description_populated"]


class CheckMacroDescriptionPopulated(BaseCheck):
    name: Literal["check_macro_description_populated"]


class CheckMacroNameMatchesFileName(BaseCheck):
    name: Literal["check_macro_name_matches_file_name"]


class CheckModelDescriptionPopulated(BaseCheck):
    name: Literal["check_model_description_populated"]


class CheckModelNames(BaseCheck):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    name: Literal["check_model_names"]
    model_name_pattern: str = Field(description="Regexp the model name must match.")


class CheckProjectName(BaseCheck):
    name: Literal["check_project_name"]
    project_name_pattern: str = Field(description="Regexp the project name must match.")


class CheckSourceHasMetaKeys(BaseCheck):
    keys: Optional[Union[Dict[str, Any], List[Any]]]
    name: Literal["check_source_has_meta_keys"]


class CheckTopLevelDirectories(BaseCheck):
    name: Literal["check_top_level_directories"]


class CheckThatDoesntExistYet(BaseCheck):
    name: Literal["check_that_doesnt_exist_yet"]


class DbtBouncerConfigFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    catalog_checks: List[
        Annotated[
            Union[CheckThatDoesntExistYet,],
            Field(discriminator="name"),
        ]
    ] = Field(default=[])
    manifest_checks: List[
        Annotated[
            Union[
                CheckMacroArgumentsDescriptionPopulated,
                CheckMacroDescriptionPopulated,
                CheckMacroNameMatchesFileName,
                CheckModelDescriptionPopulated,
                CheckModelNames,
                CheckProjectName,
                CheckSourceHasMetaKeys,
                CheckTopLevelDirectories,
            ],
            Field(discriminator="name"),
        ]
    ] = Field(default=[])

    dbt_artifacts_dir: Optional[Path] = Field(alias="dbt-artifacts-dir", default=Path("./target"))


def validate_config_file(file: Path) -> DbtBouncerConfigFile:
    logger.info("Validating config file...")
    with Path.open(file, "r") as f:
        definitions = yaml.safe_load(f)

    return DbtBouncerConfigFile(**definitions)
