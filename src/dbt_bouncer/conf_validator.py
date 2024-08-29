import importlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field
from pydantic._internal._model_construction import ModelMetaclass
from typing_extensions import Annotated

# Dynamically import all Check classes
check_files = [
    f for f in (Path(__file__).parent / "checks").glob("*/*.py") if f.is_file()
]
for check_file in check_files:
    imported_check_file = importlib.import_module(
        ".".join([x.replace(".py", "") for x in check_file.parts[-4:]]),
    )
    for obj in dir(imported_check_file):
        if isinstance(getattr(imported_check_file, obj), ModelMetaclass):
            locals()[obj] = getattr(imported_check_file, obj)

# Dynamically assemble all Check* classes
check_classes = [
    x
    for x in globals()
    if x.startswith("Check") and isinstance(globals()[x], ModelMetaclass)
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
    """Base model for the config file contents."""

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
    exclude: Optional[str] = Field(
        default=None,
        description="Regexp to match which paths to exclude.",
    )
    include: Optional[str] = Field(
        default=None,
        description="Regexp to match which paths to include.",
    )
    severity: Union[Literal["error", "warn"], None] = Field(
        default=None,
        description="Severity of the check, one of 'error' or 'warn'.",
    )


def validate_conf(config_file_contents: Dict[str, Any]) -> DbtBouncerConf:
    """Valiate the configuration and return the Pydantic model.

    Returns:
        DbtBouncerConf: The validated configuration.

    """
    logging.info("Validating conf...")

    return DbtBouncerConf(**config_file_contents)
