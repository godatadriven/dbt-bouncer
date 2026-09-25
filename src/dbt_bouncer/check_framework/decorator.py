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

Parameter validation belongs in the model, not the check body, so a
misconfigured check is a config error at load time rather than an exception
raised once per resource. Constrain a single parameter with its annotation
(``Annotated[int, Field(gt=0)]``, ``RegexPattern``). For a rule spanning
several parameters, pass ``validate=`` to ``@check``.

Example::

    from dbt_bouncer.check_framework.decorator import check, fail

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

    def _min_not_above_max(*, min_count: int, max_count: int) -> None:
        if min_count > max_count:
            raise ValueError("`min_count` must not exceed `max_count`.")

    @check(validate=_min_not_above_max)
    def check_model_xxx(model, *, min_count: int = 0, max_count: int = 10):
        ...
"""

from __future__ import annotations

import inspect
import sys
from typing import TYPE_CHECKING, Any, Literal, NoReturn, overload

if TYPE_CHECKING:
    from collections.abc import Callable

from pydantic import Field, create_model, model_validator

from dbt_bouncer.check_framework.base import BaseCheck
from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError

# Names reserved for resource / context injection, not user params.
_RESERVED_PARAMS = frozenset({"ctx"})


def fail(message: str) -> NoReturn:
    """Raise a check failure with the given message.

    ``NoReturn`` is load-bearing, not decoration: checks routinely guard on a
    value then call ``fail()``, and without it a type checker still treats the
    value as possibly-``None`` on the following line.

    Args:
        message: Human-readable description of what went wrong.

    Raises:
        DbtBouncerFailedCheckError: Always.

    """
    raise DbtBouncerFailedCheckError(message)


@overload
def check(fn: Callable[..., None]) -> type[BaseCheck]: ...


@overload
def check(
    fn: None = None,
    *,
    code: str | None = None,
    validate: Callable[..., None] | None = None,
) -> Callable[[Callable[..., None]], type[BaseCheck]]: ...


def check(
    fn: Callable[..., None] | None = None,
    *,
    code: str | None = None,
    validate: Callable[..., None] | None = None,
) -> type[BaseCheck] | Callable[[Callable[..., None]], type[BaseCheck]]:
    """Generate a ``BaseCheck`` subclass from a plain function.

    Everything is inferred from the function signature:

    - **code** — optional rule code (e.g. ``"MO001"``).
    - **name** — ``fn.__name__`` (must match YAML config ``name:`` value).
    - **iterate_over** — the first positional parameter that isn't ``ctx``.
      If there are none, the check is global (runs once with context only).
    - **params** — keyword-only arguments become Pydantic fields.
    - **ctx** — injected when the function declares it.

    ``validate`` is an optional callable for rules that span several
    parameters. It is called with the check's keyword-only parameters as
    keyword arguments once the config is loaded, and raises ``ValueError`` to
    reject them. The error is reported as a config error, before any check runs.

    Supports ``@check``, ``@check()``, and ``@check(code="MO001")`` usage.

    Returns:
        The generated ``BaseCheck`` subclass (or a decorator if called with parens).

    """
    if fn is None:
        # Called as @check() or @check(code="MO001") — return decorator.
        def wrapper(f: Callable[..., None]) -> type[BaseCheck]:
            return _build_check_class(f, code=code, validate=validate)

        return wrapper

    # Called as bare @check — fn is the decorated function.
    return _build_check_class(fn, code=code, validate=validate)


def _build_check_class(
    fn: Callable[..., None],
    code: str | None = None,
    validate: Callable[..., None] | None = None,
) -> type[BaseCheck]:
    """Build a BaseCheck subclass from the decorated function.

    Args:
        fn: The decorated check function.
        code: Optional rule code for the check.
        validate: Optional cross-parameter validator, see ``check``.

    Returns:
        The generated ``BaseCheck`` subclass.

    """
    # `Callable` has no `__name__` in the type system, but every decorated
    # check is a real function.
    name = fn.__name__  # ty: ignore[unresolved-attribute]
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

    # Extract keyword-only params → become Pydantic fields.
    param_names: list[str] = []
    fields: dict[str, Any] = {
        # `Literal[<runtime str>]` is exactly the dynamism this factory exists for.
        "name": (Literal[name], Field(default=name)),  # ty: ignore[invalid-type-form]
    }
    if code is not None:
        # Not optional: the code is part of the check's identity, so config must
        # not be able to null it out.
        fields["code"] = (Literal[code], Field(default=code))  # ty: ignore[invalid-type-form]

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
        # Testing `iterate_over` rather than `has_resource_param` (the same
        # condition) lets a type checker narrow away the `None`.
        if iterate_over is not None:
            args.append(getattr(self, iterate_over))
        if wants_ctx:
            args.append(self._ctx)
        fn(*args, **kwargs)

    validators: dict[str, Any] = {}
    if validate is not None:

        def _validate_params(self: BaseCheck) -> BaseCheck:
            validate(**{p: getattr(self, p) for p in param_names})
            return self

        validators["_validate_params"] = model_validator(mode="after")(_validate_params)

    # Convert function name to PascalCase class name.
    class_name = _to_pascal_case(name)

    cls = create_model(
        class_name,
        __base__=BaseCheck,
        __validators__=validators,
        **fields,
    )

    # Attach the execute method and class-level metadata.
    cls.execute = execute  # ty: ignore[unresolved-attribute]
    # Load-bearing despite the Pydantic `code` field above: Pydantic does not
    # expose field defaults as class attributes, and the registry, `list` CLI
    # and docs generator all read the code off the class via getattr.
    cls.code = code
    cls.iterate_over = iterate_over

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
        import logging

        logging.debug(
            f"Module {fn.__module__!r} not in sys.modules; "
            f"injecting {class_name} via stack frame fallback."
        )
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
