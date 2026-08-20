"""Utility functions for the explain CLI subcommand."""

import inspect
import typing
from typing import TYPE_CHECKING, Any, TypedDict

from pydantic_core import PydanticUndefined
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from dbt_bouncer.cli.list.utils import base_fields, category_key

if TYPE_CHECKING:
    from dbt_bouncer.check_framework.base import BaseCheck

DOCS_BASE_URL = "https://godatadriven.github.io/dbt-bouncer/checks"


class ParameterDict(TypedDict):
    """Dictionary representing one configurable check parameter."""

    default: Any
    required: bool
    type: str


class ExplainDict(TypedDict):
    """Dictionary representing an explained check."""

    category: str
    code: str | None
    description: str
    docstring: str
    documentation_url: str | None
    name: str
    parameters: dict[str, ParameterDict]


def get_check_name(check_class: type["BaseCheck"]) -> str:
    """Return the snake_case check name from the class's ``name`` Literal field.

    Args:
        check_class: The check class to inspect.

    Returns:
        str: The check name (e.g. ``check_model_names``), or the class name
        when the Literal value cannot be extracted.

    """
    name_field = check_class.model_fields.get("name")
    if name_field is not None:
        args = typing.get_args(name_field.annotation)
        if args:
            return str(args[0])
    return check_class.__name__


def get_documentation_url(check_class: type["BaseCheck"]) -> str | None:
    """Return the documentation page URL for a built-in check.

    Args:
        check_class: The check class to inspect.

    Returns:
        str | None: The page URL on the documentation website, or ``None``
        for custom checks (which have no published documentation page).

    """
    module = check_class.__module__
    prefix = "dbt_bouncer.checks."
    if not module.startswith(prefix):
        return None
    subpath = module.removeprefix(prefix).replace(".", "/")
    return f"{DOCS_BASE_URL}/{subpath}/"


def build_explain_payload(check_class: type["BaseCheck"]) -> ExplainDict:
    """Build the structured payload describing one check.

    Args:
        check_class: The check class to explain.

    Returns:
        ExplainDict: All information needed to render the explanation.

    """
    docstring = inspect.cleandoc(check_class.__doc__ or "")
    description = docstring.splitlines()[0] if docstring else ""

    parameters: dict[str, ParameterDict] = {}
    for field_name, field_info in check_class.model_fields.items():
        if field_name in base_fields or field_name == "name":
            continue
        annotation = field_info.annotation
        type_str = getattr(annotation, "__name__", None) or str(annotation)
        default = field_info.default
        parameters[field_name] = {
            "default": None if default is PydanticUndefined else default,
            "required": field_info.is_required(),
            "type": type_str,
        }

    return {
        "category": category_key(check_class),
        "code": getattr(check_class, "code", None),
        "description": description,
        "docstring": docstring,
        "documentation_url": get_documentation_url(check_class),
        "name": get_check_name(check_class),
        "parameters": parameters,
    }


def print_text_explanation(payload: ExplainDict) -> None:
    """Print a check explanation as Rich-formatted text.

    Args:
        payload: The explanation payload, as returned by
            :func:`build_explain_payload`.

    """
    console = Console()

    code = payload["code"]
    title = f"[bold white]{payload['name']}[/bold white]"
    if code:
        title = f"{title} [cyan]({code})[/cyan]"

    console.print(
        Panel(
            Text(payload["docstring"]),
            title=title,
            title_align="left",
            subtitle=f"[cyan]{payload['category']}[/cyan]",
            subtitle_align="right",
            box=box.ROUNDED,
            border_style="cyan",
        )
    )

    if payload["parameters"]:
        table = Table(
            title="[bold cyan]Parameters[/bold cyan]",
            title_justify="left",
            box=box.ROUNDED,
            border_style="cyan",
            show_header=True,
            header_style="bold cyan",
        )
        table.add_column("Name", style="bold white", no_wrap=True)
        table.add_column("Type", style="yellow")
        table.add_column("Required", justify="center")
        table.add_column("Default")

        for name, param in payload["parameters"].items():
            table.add_row(
                name,
                param["type"],
                "yes" if param["required"] else "no",
                "-" if param["default"] is None else repr(param["default"]),
            )
        console.print(table)

    if payload["documentation_url"]:
        console.print(f"Documentation: {payload['documentation_url']}")
