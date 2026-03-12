"""Decorator-based API for defining dbt-bouncer checks.

Provides a ``@check`` decorator that generates ``BaseCheck`` subclasses from
plain functions, and a ``fail()`` helper that raises the standard check failure
exception.

Example::

    from dbt_bouncer.check_decorator import check, fail

    @check("check_model_description_populated", iterate_over="model")
    def check_model_description_populated(model, ctx):
        desc = model.description or ""
        if len(desc.strip()) < 4:
            fail(f"`{model.unique_id}` does not have a populated description.")

    @check("check_model_names", iterate_over="model", params={"model_name_pattern": str})
    def check_model_names(model, ctx, *, model_name_pattern: str):
        import re
        if not re.match(model_name_pattern, str(model.name)):
            fail(f"`{model.unique_id}` does not match pattern `{model_name_pattern}`.")
"""

from __future__ import annotations

import inspect
import sys
from typing import TYPE_CHECKING, Any, Literal, get_args, get_origin

if TYPE_CHECKING:
    from collections.abc import Callable

from pydantic import Field, create_model

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


def fail(message: str) -> None:
    """Raise a check failure with the given message.

    Args:
        message: Human-readable description of what went wrong.

    Raises:
        DbtBouncerFailedCheckError: Always.

    """
    raise DbtBouncerFailedCheckError(message)


def check(
    name: str,
    *,
    iterate_over: str | None = None,
    params: dict[str, Any] | None = None,
) -> Callable[[Callable[..., None]], type[BaseCheck]]:
    """Generate a ``BaseCheck`` subclass from a plain function.

    The generated class is injected into the calling module's namespace so that
    dbt-bouncer's check discovery (``get_check_objects``) finds it via the
    normal filesystem scan.

    Args:
        name: The check name used in YAML config (e.g. ``"check_model_names"``).
            Must be unique across all checks.
        iterate_over: The resource type to iterate over (e.g. ``"model"``,
            ``"source"``).  When ``None``, the check runs once with context only.
        params: Mapping of parameter names to their types.  These become
            user-configurable Pydantic fields on the generated class.  Use a
            tuple ``(type, default)`` as the value to specify a default.

    Returns:
        A decorator that accepts a function and returns the generated class.

    """

    def decorator(fn: Callable[..., None]) -> type[BaseCheck]:
        # Build the Pydantic field definitions for create_model().
        fields: dict[str, Any] = {
            "name": (Literal[name], Field(default=name)),  # type: ignore[valid-type]
        }

        # Resource field for iterate_over detection by the runner.
        if iterate_over is not None:
            fields[iterate_over] = (Any | None, Field(default=None))

        # User-configurable parameters.
        for param_name, param_spec in (params or {}).items():
            if (
                isinstance(param_spec, tuple)
                and len(param_spec) == 2
                and _is_type(param_spec[0])
            ):
                # (type, default) pair
                param_type, param_default = param_spec
                fields[param_name] = (param_type, Field(default=param_default))
            elif _is_type(param_spec):
                # Required field — just a type
                fields[param_name] = (param_spec, ...)
            else:
                # Treat as a default value, infer type
                fields[param_name] = (type(param_spec), Field(default=param_spec))

        # Store iterate_over as a ClassVar so the runner can read it directly.
        # This is set after create_model since ClassVars can't be passed as fields.

        # Build the execute() method that delegates to the user function.
        def execute(self: BaseCheck) -> None:
            kwargs: dict[str, Any] = {p: getattr(self, p) for p in (params or {})}
            if iterate_over is not None:
                fn(getattr(self, iterate_over), self._ctx, **kwargs)
            else:
                fn(self._ctx, **kwargs)

        # Convert function name to PascalCase class name.
        class_name = _to_pascal_case(fn.__name__)  # type: ignore[union-attr]

        cls = create_model(
            class_name,
            __base__=BaseCheck,
            **fields,
        )

        # Attach the execute method.
        cls.execute = execute  # type: ignore[attr-defined]

        # Store iterate_over as a ClassVar for explicit runner lookup.
        cls.iterate_over = iterate_over  # type: ignore[attr-defined]

        # Preserve metadata.
        cls.__module__ = fn.__module__
        cls.__qualname__ = class_name
        cls.__doc__ = fn.__doc__ or f"Check: {name}"

        # Inject into the calling module's namespace for check discovery.
        caller_module = sys.modules.get(fn.__module__)
        if caller_module is not None:
            setattr(caller_module, class_name, cls)
        else:
            # Fallback: use stack frame (e.g. when __module__ is not yet in sys.modules).
            frame = inspect.stack()[1]
            frame.frame.f_globals[class_name] = cls

        return cls

    return decorator


def _to_pascal_case(snake_name: str) -> str:
    """Convert a snake_case name to PascalCase.

    Args:
        snake_name: The snake_case string.

    Returns:
        The PascalCase equivalent.

    """
    return "".join(word.capitalize() for word in snake_name.split("_"))


def _is_type(obj: Any) -> bool:
    """Check if obj is a type or a generic alias (e.g. list[str]).

    Args:
        obj: The object to check.

    Returns:
        True if obj is a type or typing generic.

    """
    if isinstance(obj, type):
        return True
    # Handle typing generics like list[str], Optional[int], etc.
    return get_origin(obj) is not None or (
        hasattr(obj, "__args__") and bool(get_args(obj))
    )
