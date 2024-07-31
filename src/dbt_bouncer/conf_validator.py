import importlib
from pathlib import Path
from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field
from pydantic._internal._model_construction import ModelMetaclass
from typing_extensions import Annotated

from dbt_bouncer.logger import logger

# Dynamically import all Check classes
check_files = [f for f in (Path(__file__).parent / "checks").glob("*/*.py") if f.is_file()]
for check_file in check_files:
    imported_check_file = importlib.import_module(
        ".".join([x.replace(".py", "") for x in check_file.parts[-4:]])
    )
    for obj in dir(imported_check_file):
        if isinstance(getattr(imported_check_file, obj), ModelMetaclass):
            locals()[obj] = getattr(imported_check_file, obj)

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

# Run result checks
run_results_check_classes = [
    x for x in check_classes if globals()[x].__module__.split(".")[-2] == "run_results"
]
RunResultsCheckConfigs = Annotated[  # type: ignore[valid-type]
    Union[tuple(run_results_check_classes)],
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
    run_results_checks: List[  # type: ignore[valid-type]
        Annotated[
            RunResultsCheckConfigs,
            Field(discriminator="name"),
        ]
    ] = Field(default=[])
    dbt_artifacts_dir: Optional[str] = Field(default="./target")


def validate_conf(conf) -> DbtBouncerConf:
    logger.info("Validating conf...")

    return DbtBouncerConf(**conf)
