import inspect
import operator
import os
from functools import reduce
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated

from dbt_bouncer.utils import get_check_objects


def get_check_types(
    check_type: str,
    custom_checks_dir: Path | None = None,
) -> list[Any]:
    """Get the check types from the check categories.

    Args:
        check_type: The check category subdirectory name (e.g. "catalog", "manifest", "run_results").
        custom_checks_dir: Path to a directory containing custom checks.

    Returns:
        list[str]: The check types.

    """

    def _get_category(check_class: type) -> str:
        module = check_class.__module__
        if module.startswith("dbt_bouncer.checks."):
            return module.split(".")[2]
        return Path(inspect.getfile(check_class)).parts[-2]

    filtered_classes = [
        x
        for x in get_check_objects(custom_checks_dir)
        if _get_category(x) == check_type
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


def create_bouncer_conf_class(
    custom_checks_dir: Path | None = None,
) -> type[DbtBouncerConfBase]:
    """Create a DbtBouncerConf Pydantic model class with check types resolved at call time.

    This factory avoids module-level class definitions that depend on
    global mutable state and removes the need for importlib.reload() hacks.

    Args:
        custom_checks_dir: Path to a directory containing custom checks.

    Returns:
        type[DbtBouncerConfBase]: A Pydantic model class with the appropriate check type fields.

    """
    catalog_type = get_check_types(
        check_type="catalog", custom_checks_dir=custom_checks_dir
    )
    manifest_type = get_check_types(
        check_type="manifest", custom_checks_dir=custom_checks_dir
    )
    run_results_type = get_check_types(
        check_type="run_results", custom_checks_dir=custom_checks_dir
    )

    class DbtBouncerConf(DbtBouncerConfBase):
        """Config file contents for dbt-bouncer."""

        catalog_checks: catalog_type = Field(default=[])  # type: ignore[valid-type]
        manifest_checks: manifest_type = Field(default=[])  # type: ignore[valid-type]
        run_results_checks: run_results_type = Field(default=[])  # type: ignore[valid-type]

    return DbtBouncerConf


# Module-level class for TYPE_CHECKING imports and backwards compatibility
DbtBouncerConfAllCategories = create_bouncer_conf_class()
DbtBouncerConf = DbtBouncerConfAllCategories
