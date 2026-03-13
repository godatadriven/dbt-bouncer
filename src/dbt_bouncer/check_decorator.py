"""Decorator-based API for defining dbt-bouncer checks.

Provides a ``@check`` decorator that generates ``BaseCheck`` subclasses from
plain functions, and a ``fail()`` helper that raises the standard check failure
exception.

Parameters are inferred from the function signature — keyword-only arguments
(after ``*``) become user-configurable Pydantic fields on the generated class.
``ctx`` is injected automatically only when the function declares it.

Example::

    from dbt_bouncer.check_decorator import check, fail

    @check("check_model_description_populated", iterate_over="model")
    def check_model_description_populated(model):
        desc = model.description or ""
        if len(desc.strip()) < 4:
            fail(f"`{model.unique_id}` does not have a populated description.")

    @check("check_model_names", iterate_over="model")
    def check_model_names(model, *, model_name_pattern: str):
        import re
        if not re.match(model_name_pattern, str(model.name)):
            fail(f"`{model.unique_id}` does not match pattern `{model_name_pattern}`.")

    @check("check_model_documentation_coverage")
    def check_model_documentation_coverage(ctx, *, min_pct: int = 100):
        ...  # context-only check, no iterate_over
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

# Names reserved for resource / context injection, not user params.
_RESERVED_PARAMS = frozenset({"ctx"})


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
) -> Callable[[Callable[..., None]], type[BaseCheck]]:
    """Generate a ``BaseCheck`` subclass from a plain function.

    Parameters are inferred from the function's keyword-only arguments
    (those after ``*``).  The ``ctx`` parameter is injected automatically
    when declared.

    Args:
        name: The check name used in YAML config (e.g. ``"check_model_names"``).
        iterate_over: The resource type to iterate over (e.g. ``"model"``,
            ``"source"``).  When ``None``, the check runs once with context only.

    Returns:
        A decorator that accepts a function and returns the generated class.

    """

    def decorator(fn: Callable[..., None]) -> type[BaseCheck]:
        sig = inspect.signature(fn)
        fn_params = sig.parameters

        # Detect whether the function wants ctx injected.
        wants_ctx = "ctx" in fn_params

        # Detect whether the function receives the resource as first positional arg.
        # Positional params that aren't "ctx" and aren't keyword-only are the resource.
        positional_names = [
            p.name
            for p in fn_params.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            and p.name not in _RESERVED_PARAMS
        ]
        has_resource_param = bool(positional_names) and iterate_over is not None

        # Extract keyword-only params → become Pydantic fields.
        param_names: list[str] = []
        fields: dict[str, Any] = {
            "name": (Literal[name], Field(default=name)),  # type: ignore[valid-type]
        }

        # Resource field for iterate_over detection by the runner.
        if iterate_over is not None:
            fields[iterate_over] = (Any | None, Field(default=None))

        for param_name, param in fn_params.items():
            if param.kind != param.KEYWORD_ONLY:
                continue

            param_names.append(param_name)
            annotation = param.annotation

            if annotation is inspect.Parameter.empty:
                annotation = Any

            if param.default is not inspect.Parameter.empty:
                fields[param_name] = (annotation, Field(default=param.default))
            else:
                fields[param_name] = (annotation, ...)

        # Build the execute() method that delegates to the user function.
        def execute(self: BaseCheck) -> None:
            kwargs: dict[str, Any] = {p: getattr(self, p) for p in param_names}
            args: list[Any] = []
            if has_resource_param:
                args.append(getattr(self, iterate_over))  # type: ignore[arg-type]
            if wants_ctx:
                args.append(self._ctx)
            fn(*args, **kwargs)

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
