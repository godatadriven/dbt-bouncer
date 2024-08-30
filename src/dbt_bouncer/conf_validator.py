import logging
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated

from dbt_bouncer.utils import get_check_objects

check_classes: List[str] = [
    x for x in get_check_objects() if x.split(".")[-1].startswith("Check")
]

# Catalog checks
catalog_check_classes = [x for x in check_classes if x.split(".")[-3] == "catalog"]
CatalogCheckConfigs = Annotated[  # type: ignore[valid-type]
    Union[tuple(catalog_check_classes)],
    Field(discriminator="name"),
]

# Manifest checks
manifest_check_classes = [x for x in check_classes if x.split(".")[-3] == "manifest"]
ManifestCheckConfigs = Annotated[  # type: ignore[valid-type]
    Union[tuple(manifest_check_classes)],
    Field(discriminator="name"),
]

# Run result checks
run_results_check_classes = [
    x for x in check_classes if x.split(".")[-3] == "run_results"
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

    DbtBouncerConf.model_rebuild()
    return DbtBouncerConf(**config_file_contents)
