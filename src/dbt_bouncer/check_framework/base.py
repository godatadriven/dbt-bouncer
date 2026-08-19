"""Base check model that all dbt-bouncer checks inherit from."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator

from dbt_bouncer.enums import CheckSeverity, Materialization, RuleCode
from dbt_bouncer.utils import is_description_populated


class BaseCheck(BaseModel):
    """Base class for all checks."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True, defer_build=True, extra="forbid"
    )

    # The `str` arm is deliberately unconstrained: built-in checks are narrowed
    # to a `Literal` by the `@check` decorator, but custom checks registered
    # via `@check` are free to use their own code scheme.
    code: RuleCode | str | None = Field(
        default=None,
        description="Unique rule code for the check (e.g. 'MO001').",
    )
    description: str | None = Field(
        default=None,
        description="Description of what the check does and why it is implemented.",
    )
    exclude: str | list[str] | None = Field(
        default=None,
        description="Regexp(s) to match which paths to exclude. A list matches if any pattern matches.",
    )
    include: str | list[str] | None = Field(
        default=None,
        description="Regexp(s) to match which paths to include. A list matches if any pattern matches.",
    )
    index: int | None = Field(
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    materialization: Materialization | None = Field(
        default=None,
        description="Limit check to models with the specified materialization.",
    )
    selector: str | None = Field(
        default=None,
        description="dbt-style node selector limiting which resources the check runs against (e.g. 'tag:finance', '+orders', 'stg_*,tag:critical').",
    )
    severity: CheckSeverity | None = Field(
        default=CheckSeverity.ERROR,
        description="Severity of the check, one of 'error' or 'warn'.",
    )

    @field_validator("selector")
    @classmethod
    def _validate_selector(cls, value: str | None) -> str | None:
        """Reject syntactically invalid selectors at config-validation time.

        Returns:
            str | None: The validated selector string.

        """
        if value is not None:
            from dbt_bouncer.selectors import parse_selector

            parse_selector(value)
        return value

    # NB: the twelve per-resource fields (``model``, ``seed``, ``catalog_node``,
    # ...) are deliberately NOT declared here. Every check binds exactly one
    # resource, and the ``@check`` decorator adds that field to the generated
    # subclass as ``Any | None``, setting ``iterate_over`` to its name.
    # Declaring all twelve on the base made every generated subclass re-collect
    # and copy 19 inherited fields instead of 7 -- measured at ~29% of total
    # check-class construction, which is the single largest cost in a warm run.
    # ``set_resource`` writes straight into the instance ``__dict__``, so
    # nothing here needs a slot reserved for it.

    _ctx: Any = PrivateAttr(default=None)
    _min_description_length: ClassVar[int] = 4

    # Set by the ``@check`` decorator to the check's iterate-over resource name
    # (``None`` for context-only checks). Declared here so it is a real, typed
    # class attribute rather than one conjured onto the generated subclass.
    # ``ClassVar`` keeps Pydantic from treating it as a model field.
    iterate_over: ClassVar[str | None] = None

    def set_context(self, ctx: Any) -> None:
        """Set the execution context for this check instance.

        Args:
            ctx: A ``CheckContext`` holding parsed dbt artifacts.

        """
        self._ctx = ctx

    def set_resource(self, resource: Any, iterate_over_value: str) -> None:
        """Set the per-iteration resource on this check instance.

        Args:
            resource: The dbt resource wrapper object (e.g. DbtBouncerModel).
            iterate_over_value: The field name to set (e.g. "model", "seed").

        """
        if isinstance(resource, dict):
            inner = resource.get(iterate_over_value, resource)
        else:
            inner = getattr(resource, iterate_over_value, resource)
        object.__setattr__(self, iterate_over_value, inner)

    # Helper methods
    def _is_description_populated(
        self, description: str, min_description_length: int | None
    ) -> bool:
        """Check if a description is populated.

        Args:
            description (str): Description.
            min_description_length (int): Minimum length of the description.

        Returns:
            bool: Whether a description is validly populated.

        """
        return is_description_populated(
            description=description,
            min_description_length=min_description_length
            or self._min_description_length,
        )
