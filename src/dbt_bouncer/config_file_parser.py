from pathlib import Path
from typing import Any, ClassVar, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated

from dbt_bouncer.utils import clean_path_str


class DbtBouncerConf(BaseModel):
    """Base model for the config file contents."""

    model_config = ConfigDict(extra="forbid")

    from dbt_bouncer.utils import get_check_objects

    check_classes: List[Dict[str, Union[Any, Path]]] = [
        {
            "class": getattr(x, x.__name__),
            "source_file": Path(clean_path_str(x.__file__)),
        }
        for x in get_check_objects()
    ]

    # Catalog checks
    catalog_check_classes: ClassVar = [
        x["class"] for x in check_classes if x["source_file"].parts[-2] == "catalog"
    ]

    CatalogCheckConfigs: ClassVar = Annotated[
        Union[tuple(catalog_check_classes)],
        Field(discriminator="name"),
    ]

    # Manifest checks
    manifest_check_classes: ClassVar = [
        x["class"] for x in check_classes if x["source_file"].parts[-2] == "manifest"
    ]

    ManifestCheckConfigs: ClassVar = Annotated[
        Union[tuple(manifest_check_classes)],
        Field(discriminator="name"),
    ]

    # Run result checks
    run_results_check_classes: ClassVar = [
        x["class"] for x in check_classes if x["source_file"].parts[-2] == "run_results"
    ]

    RunResultsCheckConfigs: ClassVar = Annotated[
        Union[tuple(run_results_check_classes)],
        Field(discriminator="name"),
    ]

    catalog_checks: List[
        Annotated[
            CatalogCheckConfigs,
            Field(discriminator="name"),
        ]
    ] = Field(default=[])

    manifest_checks: List[
        Annotated[
            ManifestCheckConfigs,
            Field(discriminator="name"),
        ]
    ] = Field(default=[])
    run_results_checks: List[
        Annotated[
            RunResultsCheckConfigs,
            Field(discriminator="name"),
        ]
    ] = Field(default=[])

    custom_checks_dir: Optional[str] = Field(
        default=None,
        description="Path to a directory containing custom checks.",
    )
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
