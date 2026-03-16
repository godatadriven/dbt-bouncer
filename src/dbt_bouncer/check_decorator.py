"""Decorator-based API for defining dbt-bouncer checks.

Provides a ``@check`` decorator that generates ``BaseCheck`` subclasses from
plain functions, and a ``fail()`` helper that raises the standard check failure
exception.

Everything is inferred from the function signature:

- **name** — the function name (used in YAML config).
- **iterate_over** — the first positional parameter (excluding ``ctx``).
  If there are no positional params (or only ``ctx``), the check is global.
- **params** — keyword-only arguments become user-configurable Pydantic fields.
- **ctx** — injected automatically only when the function declares it.

Example::

    from dbt_bouncer.check_decorator import check, fail

    @check
    def check_model_description_populated(model):
        desc = model.description or ""
        if len(desc.strip()) < 4:
            fail(f"`{model.unique_id}` does not have a populated description.")

    @check
    def check_model_names(model, *, model_name_pattern: str):
        import re
        if not re.match(model_name_pattern, str(model.name)):
            fail(f"`{model.unique_id}` does not match pattern `{model_name_pattern}`.")

    @check
    def check_model_documentation_coverage(ctx, *, min_pct: int = 100):
        ...  # context-only check, no iterate_over
"""

from __future__ import annotations

import inspect
import sys
from typing import TYPE_CHECKING, Any, Literal

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
    fn: Callable[..., None] | None = None,
) -> type[BaseCheck] | Callable[[Callable[..., None]], type[BaseCheck]]:
    """Generate a ``BaseCheck`` subclass from a plain function.

    Everything is inferred from the function signature:

    - **name** — ``fn.__name__`` (must match YAML config ``name:`` value).
    - **iterate_over** — the first positional parameter that isn't ``ctx``.
      If there are none, the check is global (runs once with context only).
    - **params** — keyword-only arguments become Pydantic fields.
    - **ctx** — injected when the function declares it.

    Supports both ``@check`` and ``@check()`` usage.

    Returns:
        The generated ``BaseCheck`` subclass (or a decorator if called with parens).

    """
    if fn is None:
        # Called as @check() with parens — return decorator.
        def wrapper(f: Callable[..., None]) -> type[BaseCheck]:
            return _build_check_class(f)

        return wrapper

    # Called as bare @check — fn is the decorated function.
    return _build_check_class(fn)


def _build_check_class(fn: Callable[..., None]) -> type[BaseCheck]:
    """Build a BaseCheck subclass from the decorated function.

    Returns:
        The generated ``BaseCheck`` subclass.

    """
    name = fn.__name__  # type: ignore[union-attr]
    sig = inspect.signature(fn)
    fn_params = sig.parameters

    # Detect whether the function wants ctx injected.
    wants_ctx = "ctx" in fn_params

    # First non-ctx positional param is the resource → becomes iterate_over.
    positional_names = [
        p.name
        for p in fn_params.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        and p.name not in _RESERVED_PARAMS
    ]
    iterate_over: str | None = positional_names[0] if positional_names else None
    has_resource_param = iterate_over is not None

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
    class_name = _to_pascal_case(name)

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

    # Inject into the calling module's namespace so check discovery
    # (which scans module attributes for BaseCheck subclasses) finds the
    # generated class just as if it had been defined with ``class ...``.
    caller_module = sys.modules.get(fn.__module__)
    if caller_module is not None:
        setattr(caller_module, class_name, cls)
    else:
        # Fallback: use stack frame (e.g. when __module__ is not yet in sys.modules).
        frame = inspect.stack()[1]
        frame.frame.f_globals[class_name] = cls

    return cls


def _to_pascal_case(snake_name: str) -> str:
    """Convert a snake_case name to PascalCase.

    Args:
        snake_name: The snake_case string.

    Returns:
        The PascalCase equivalent.

    """
    return "".join(word.capitalize() for word in snake_name.split("_"))
