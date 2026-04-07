"""Utility functions for the list CLI subcommand."""

import itertools
from typing import TypedDict

import typer


def category_key(check_class: type) -> str:
    """Return the display category name segment from the module path.

    Args:
        check_class: The check class whose module path is inspected.

    Returns:
        str: The category name segment.

    """
    # e.g. "dbt_bouncer.checks.manifest.check_models" -> "manifest"
    parts = check_class.__module__.split(".")
    return parts[2] if len(parts) > 2 else "other"


# Fields inherited from BaseCheck that are not check-specific parameters.
base_fields = frozenset(
    {
        "catalog_node",
        "catalog_source",
        "description",
        "exclude",
        "exposure",
        "exposures_by_unique_id",
        "include",
        "index",
        "macro",
        "manifest_obj",
        "materialization",
        "model",
        "models_by_unique_id",
        "run_result",
        "seed",
        "semantic_model",
        "severity",
        "snapshot",
        "source",
        "sources_by_unique_id",
        "test",
        "tests_by_unique_id",
        "unit_test",
    }
)


def get_check_params(check_class: type) -> dict[str, str]:
    """Return configurable parameter names and their type annotations.

    Args:
        check_class: The check class whose model fields are inspected.

    Returns:
        dict[str, str]: Mapping of field name to type string.

    """
    params: dict[str, str] = {}
    for field_name, field_info in check_class.model_fields.items():  # type: ignore[attr-defined]
        if field_name in base_fields:
            continue
        annotation = field_info.annotation
        type_str = getattr(annotation, "__name__", None) or str(annotation)
        params[field_name] = type_str
    return params


class CheckDict(TypedDict):
    """Dictionary representing a check."""

    description: str
    name: str
    parameters: dict[str, str]


def build_checks_payload(
    checks: list[type], category_labels: dict[str, str]
) -> dict[str, list[CheckDict]]:
    """Build the checks payload grouped by category.

    Args:
        checks: List of check classes.
        category_labels: Mapping from category to category label.

    Returns:
        The checks payload grouped by category.

    """
    result: dict[str, list[CheckDict]] = {}
    for category, group in itertools.groupby(checks, key=category_key):
        label = category_labels.get(category, category)
        result[label] = []
        for check_class in group:
            docstring = (check_class.__doc__ or "").strip()
            description = docstring.splitlines()[0] if docstring else ""
            result[label].append(
                {
                    "description": description,
                    "name": check_class.__name__,
                    "parameters": get_check_params(check_class),
                }
            )
    return result


def print_text_checks(checks_payload: dict[str, list[CheckDict]]) -> None:
    """Print checks in text format.

    Args:
        checks_payload: Checks grouped by category label, as returned by
            :func:`build_checks_payload`.

    """
    for label, checks in checks_payload.items():
        typer.echo(f"{label}:")
        for check in checks:
            params = check["parameters"]
            params_text = (
                "\n".join(
                    f"        {name}: {type_str}" for name, type_str in params.items()
                )
                if params
                else "        (none)"
            )
            typer.echo(
                f"  {check['name']}:\n      {check['description']}\n      Parameters:\n{params_text}\n"
            )
