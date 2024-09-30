from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated

from dbt_bouncer.utils import clean_path_str


def get_check_types(
    check_type: List[
        Literal["catalog_checks", "manifest_checks", "run_results_checks"]
    ],
) -> List[Any]:
    """Get the check types from the check categories.

    Args:
        check_type: List[Literal["catalog_checks", "manifest_checks", "run_results_checks"]]

    Returns:
        List[str]: The check types.

    """
    from dbt_bouncer.utils import get_check_objects

    check_classes: List[Dict[str, Union[Any, Path]]] = [
        {
            "class": getattr(x, x.__name__),
            "source_file": Path(clean_path_str(x.__file__)),
        }
        for x in get_check_objects()
    ]
    return List[  # type: ignore[misc, return-value]
        Annotated[
            Annotated[
                Union[
                    tuple(
                        x["class"]
                        for x in check_classes
                        if x["source_file"].parts[-2] == check_type
                    )
                ],
                Field(discriminator="name"),
            ],
            Field(discriminator="name"),
        ]
    ]


class DbtBouncerConfBase(BaseModel):
    """Base model for the config file contents."""

    model_config = ConfigDict(extra="forbid")

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


class DbtBouncerConfAllCategories(DbtBouncerConfBase):
    """Config file contents when all categories are used."""

    model_config = ConfigDict(extra="forbid")

    catalog_checks: get_check_types(check_type="catalog") = Field(default=[])  # type: ignore[valid-type]
    manifest_checks: get_check_types(check_type="manifest") = Field(default=[])  # type: ignore[valid-type]
    run_results_checks: get_check_types(check_type="run_results") = Field(default=[])  # type: ignore[valid-type]

    custom_checks_dir: Optional[str] = Field(
        default=None,
        description="Path to a directory containing custom checks.",
    )


class DbtBouncerConfCatalogManifest(DbtBouncerConfBase):
    """Config file contents when catalog and manifest are used."""

    model_config = ConfigDict(extra="forbid")

    catalog_checks: get_check_types(check_type="catalog") = Field(default=[])  # type: ignore[valid-type]
    manifest_checks: get_check_types(check_type="manifest") = Field(default=[])  # type: ignore[valid-type]


class DbtBouncerConfCatalogOnly(DbtBouncerConfBase):
    """Config file contents when catalog only is used."""

    model_config = ConfigDict(extra="forbid")

    catalog_checks: get_check_types(check_type="catalog") = Field(default=[])  # type: ignore[valid-type]


class DbtBouncerConfCatalogRunResults(DbtBouncerConfBase):
    """Config file contents when catalog and run_results are used."""

    model_config = ConfigDict(extra="forbid")

    catalog_checks: get_check_types(check_type="catalog") = Field(default=[])  # type: ignore[valid-type]
    run_results_checks: get_check_types(check_type="run_results") = Field(default=[])  # type: ignore[valid-type]


class DbtBouncerConfManifestOnly(DbtBouncerConfBase):
    """Config file contents when manifest only is used."""

    model_config = ConfigDict(extra="forbid")

    manifest_checks: get_check_types(check_type="manifest") = Field(default=[])  # type: ignore[valid-type]


class DbtBouncerConfManifestRunResults(DbtBouncerConfBase):
    """Config file contents when manifest and run_results are used."""

    model_config = ConfigDict(extra="forbid")

    manifest_checks: get_check_types(check_type="manifest") = Field(default=[])  # type: ignore[valid-type]
    run_results_checks: get_check_types(check_type="run_results") = Field(default=[])  # type: ignore[valid-type]


class DbtBouncerConfRunResultsOnly(DbtBouncerConfBase):
    """Config file contents when run_results only is used."""

    model_config = ConfigDict(extra="forbid")

    run_results_checks: get_check_types(check_type="run_results") = Field(default=[])  # type: ignore[valid-type]
