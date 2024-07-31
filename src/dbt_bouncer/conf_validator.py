from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field
from pydantic._internal._model_construction import ModelMetaclass
from typing_extensions import Annotated

from dbt_bouncer.checks.catalog.check_columns import *  # noqa
from dbt_bouncer.checks.manifest.check_macros import *  # noqa
from dbt_bouncer.checks.manifest.check_metadata import *  # noqa
from dbt_bouncer.checks.manifest.check_models import *  # noqa
from dbt_bouncer.checks.manifest.check_project_directories import *  # noqa
from dbt_bouncer.checks.manifest.check_sources import *  # noqa
from dbt_bouncer.logger import logger

# Dynamically assemble all Check* classes
check_classes = [
    x for x in globals() if x.startswith("Check") and isinstance(globals()[x], ModelMetaclass)
]

# Catalog checks
catalog_check_classes = [
    x for x in check_classes if globals()[x].__module__.split(".")[-2] == "catalog"
]
CatalogCheckConfigs = Annotated[  # type: ignore[valid-type]
    Union[tuple(catalog_check_classes)],
    Field(discriminator="name"),
]

# Manifest checks
manifest_check_classes = [
    x for x in check_classes if globals()[x].__module__.split(".")[-2] == "manifest"
]
ManifestCheckConfigs = Annotated[  # type: ignore[valid-type]
    Union[tuple(manifest_check_classes)],
    Field(discriminator="name"),
]


class DbtBouncerConf(BaseModel):
    model_config = ConfigDict(extra="forbid")

    catalog_checks: List[  # type: ignore[valid-type]
        Annotated[
            CatalogCheckConfigs,
            Field(discriminator="name"),
        ]
    ] = Field(default=[])
    manifest_checks: List[  # type: ignore[valid-type]
        Annotated[
            ManifestCheckConfigs,
            Field(discriminator="name"),
        ]
    ] = Field(default=[])
    dbt_artifacts_dir: Optional[str] = Field(default="./target")


def validate_conf(conf) -> DbtBouncerConf:
    logger.info("Validating conf...")

    return DbtBouncerConf(**conf)
