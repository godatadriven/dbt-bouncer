import inspect
import operator
import os
from functools import lru_cache, reduce
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, create_model
from typing_extensions import Annotated

from dbt_bouncer.utils import get_check_objects


def _get_category(check_class: type) -> str:
    """Determine the category subdirectory for a check class.

    Args:
        check_class: A check class.

    Returns:
        str: The category subdirectory name (e.g. "catalog", "manifest").

    """
    module = check_class.__module__
    if module.startswith("dbt_bouncer.checks."):
        return module.split(".")[2]
    return Path(inspect.getfile(check_class)).parts[-2]


def get_check_types(
    check_type: str,
    custom_checks_dir: Path | None = None,
    check_objects: list[Any] | None = None,
) -> list[Any]:
    """Get the check types from the check categories.

    Args:
        check_type: The check category subdirectory name (e.g. "catalog", "manifest", "run_results").
        custom_checks_dir: Path to a directory containing custom checks.
        check_objects: Pre-loaded check classes. When provided, skips the
            expensive ``get_check_objects()`` call.

    Returns:
        list[str]: The check types.

    """
    source = (
        check_objects
        if check_objects is not None
        else get_check_objects(custom_checks_dir)
    )
    filtered_classes = [x for x in source if _get_category(x) == check_type]
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


@lru_cache(maxsize=None)
def create_bouncer_conf_class(
    custom_checks_dir: Path | None = None,
    check_categories: frozenset[str] | None = None,
) -> type[DbtBouncerConfBase]:
    """Create a DbtBouncerConf Pydantic model class with check types resolved at call time.

    This factory avoids module-level class definitions that depend on
    global mutable state and removes the need for importlib.reload() hacks.

    Args:
        custom_checks_dir: Path to a directory containing custom checks.
        check_categories: Configured check categories. When provided, only
            fields for these categories are built with discriminated unions;
            unconfigured categories default to ``list[Any]``.

    Returns:
        type[DbtBouncerConfBase]: A Pydantic model class with the appropriate check type fields.

    """
    return _create_conf_class(
        custom_checks_dir=custom_checks_dir,
        check_categories=check_categories,
    )


def _create_conf_class(
    custom_checks_dir: Path | None = None,
    check_categories: frozenset[str] | None = None,
    check_objects: list[Any] | None = None,
) -> type[DbtBouncerConfBase]:
    """Create a DbtBouncerConf model class, optionally using pre-loaded check objects.

    Accept an optional pre-loaded ``check_objects`` list to avoid the
    expensive full module scan when targeted imports have already been done.

    Args:
        custom_checks_dir: Path to a directory containing custom checks.
        check_categories: Configured check categories.
        check_objects: Pre-loaded check classes (from targeted imports).

    Returns:
        type[DbtBouncerConfBase]: A Pydantic model class.

    """
    category_to_check_type = {
        "catalog_checks": "catalog",
        "manifest_checks": "manifest",
        "run_results_checks": "run_results",
    }

    fields: dict[str, tuple[Any, Any]] = {}
    for category, check_type in category_to_check_type.items():
        if check_categories is None or category in check_categories:
            resolved_type = get_check_types(
                check_type=check_type,
                custom_checks_dir=custom_checks_dir,
                check_objects=check_objects,
            )
        else:
            resolved_type = list[Any]
        fields[category] = (resolved_type, Field(default=[]))

    return create_model(  # type: ignore[call-overload]
        "DbtBouncerConf",
        __base__=DbtBouncerConfBase,
        **fields,
    )
