from pathlib import Path
from typing import List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field
from pydantic._internal._model_construction import ModelMetaclass
from typing_extensions import Annotated

from dbt_bouncer.checks.manifest.check_macros import *  # noqa
from dbt_bouncer.checks.manifest.check_metadata import *  # noqa
from dbt_bouncer.checks.manifest.check_models import *  # noqa
from dbt_bouncer.checks.manifest.check_project_directories import *  # noqa
from dbt_bouncer.checks.manifest.check_sources import *  # noqa
from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.logger import logger

# Dynamically assemble all Check* classes
manifest_check_classes = [
    x for x in globals() if x.startswith("Check") and isinstance(globals()[x], ModelMetaclass)
]
ManifestCheckConfigs = Annotated[  # type: ignore[valid-type]
    Union[tuple(manifest_check_classes)],
    Field(discriminator="name"),
]


class CheckThatDoesntExistYet(BaseCheck):
    name: Literal["check_that_doesnt_exist_yet"]


class DbtBouncerConf(BaseModel):
    model_config = ConfigDict(extra="forbid")

    catalog_checks: List[
        Annotated[
            Union[CheckThatDoesntExistYet,],
            Field(discriminator="name"),
        ]
    ] = Field(default=[])
    manifest_checks: List[  # type: ignore[valid-type]
        Annotated[
            ManifestCheckConfigs,
            Field(discriminator="name"),
        ]
    ] = Field(default=[])
    dbt_artifacts_dir: Optional[Path] = Field(default=Path("./target"))


def validate_conf(conf) -> DbtBouncerConf:
    logger.info("Validating conf...")

    return DbtBouncerConf(**conf)
