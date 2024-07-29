from pathlib import Path
from typing import List, Optional, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field
from pydantic._internal._model_construction import ModelMetaclass
from typing_extensions import Annotated

from dbt_bouncer.checks.check_macros import *  # noqa
from dbt_bouncer.checks.check_metadata import *  # noqa
from dbt_bouncer.checks.check_models import *  # noqa
from dbt_bouncer.checks.check_project_directories import *  # noqa
from dbt_bouncer.checks.check_sources import *  # noqa
from dbt_bouncer.logger import logger

# Dynamically assemble all Check* classes
check_classes = [
    x for x in globals() if x.startswith("Check") and isinstance(globals()[x], ModelMetaclass)
]

CheckConfigs = Annotated[  # type: ignore[valid-type]
    Union[tuple(check_classes)],
    Field(discriminator="name"),
]


class DbtBouncerConfigFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    checks: List[CheckConfigs]  # type: ignore[valid-type]
    dbt_artifacts_dir: Optional[Path] = Field(alias="dbt-artifacts-dir", default=Path("./target"))


def validate_config_file(file: Path) -> DbtBouncerConfigFile:
    logger.info("Validating config file...")
    with Path.open(file, "r") as f:
        definitions = yaml.safe_load(f)

    return DbtBouncerConfigFile(**definitions)
