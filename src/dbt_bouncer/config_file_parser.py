import inspect
import operator
import os
from functools import reduce
from pathlib import Path
from typing import Any, Literal

import click
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated

from dbt_bouncer.global_context import get_context
from dbt_bouncer.utils import clean_path_str, get_check_objects


def get_check_types(
    check_type: list[
        Literal["catalog_checks", "manifest_checks", "run_results_checks"]
    ],
) -> list[Any]:
    """Get the check types from the check categories.

    Args:
        check_type: list[Literal["catalog_checks", "manifest_checks", "run_results_checks"]]

    Returns:
        list[str]: The check types.

    """
    try:
        ctx = get_context()
        if ctx:
            config_file_path = ctx.config_file_path
            custom_checks_dir = ctx.custom_checks_dir
        else:
            click_ctx = click.get_current_context()
            config_file_path = click_ctx.obj.get("config_file_path")
            custom_checks_dir = click_ctx.obj.get("custom_checks_dir")

        if custom_checks_dir:
            custom_checks_dir = config_file_path.parent / custom_checks_dir
    except (RuntimeError, AttributeError, KeyError):
        custom_checks_dir = None

    check_classes: list[dict[str, Any | Path]] = [
        {
            "class": x,
            "source_file": Path(clean_path_str(inspect.getfile(x))),
        }
        for x in get_check_objects(custom_checks_dir)
    ]

    filtered_classes = [
        x["class"] for x in check_classes if x["source_file"].parts[-2] == check_type
    ]
    if not filtered_classes:
        return list[Any]  # type: ignore[return-value]

    return list[  # type: ignore[misc, return-value]
        Annotated[
            Annotated[
                reduce(operator.or_, filtered_classes),  # type: ignore
                Field(discriminator="name"),
            ],
            Field(discriminator="name"),
        ]
    ]


class DbtBouncerConfBase(BaseModel):
    """Base model for the config file contents."""

    model_config = ConfigDict(extra="forbid")

    custom_checks_dir: str | None = Field(
        default=None,
        description="Path to a directory containing custom checks.",
    )
    dbt_artifacts_dir: str | None = Field(
        default_factory=lambda: (
            f"{os.getenv('DBT_PROJECT_DIR')}/target"
            if os.getenv("DBT_PROJECT_DIR")
            else "./target"
        )
    )
    exclude: str | None = Field(
        default=None,
        description="Regexp to match which paths to exclude.",
    )
    include: str | None = Field(
        default=None,
        description="Regexp to match which paths to include.",
    )
    package_name: str | None = Field(
        default=None, description="If you want to run `dbt-bouncer` against a package."
    )
    severity: Literal["error", "warn"] | None = Field(
        default=None,
        description="Severity of the check, one of 'error' or 'warn'.",
    )


class DbtBouncerConfAllCategories(DbtBouncerConfBase):
    """Config file contents when all categories are used."""

    model_config = ConfigDict(extra="forbid")

    catalog_checks: get_check_types(check_type="catalog") = Field(default=[])  # type: ignore[valid-type]
    manifest_checks: get_check_types(check_type="manifest") = Field(default=[])  # type: ignore[valid-type]
    run_results_checks: get_check_types(check_type="run_results") = Field(default=[])  # type: ignore[valid-type]

    custom_checks_dir: str | None = Field(
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
