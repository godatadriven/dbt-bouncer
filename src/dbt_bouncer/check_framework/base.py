"""Base check model that all dbt-bouncer checks inherit from."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

if TYPE_CHECKING:
    # Only referenced by the ``_require_*`` return annotations, which are lazy
    # under ``from __future__ import annotations``. Previously these had to be
    # imported at runtime so Pydantic could resolve the per-resource field
    # types; those fields are gone, so the import can go with them.
    from dbt_bouncer.artifact_types import (
        CatalogNodeEntry,
        ExposureNode,
        MacroNode,
        ManifestWrapper,
        ModelNode,
        RunResultEntry,
        SeedNode,
        SemanticModelNode,
        SnapshotNode,
        SourceNode,
        TestNode,
        UnitTestNode,
    )

from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError
from dbt_bouncer.enums import CheckSeverity, Materialization, RuleCode
from dbt_bouncer.utils import is_description_populated


class BaseCheck(BaseModel):
    """Base class for all checks."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True, defer_build=True, extra="forbid"
    )

    # The `str` arm is deliberately unconstrained: built-in checks are narrowed
    # to a `Literal` by the `@check` decorator, so this only applies to
    # class-based plugin checks, which are free to use their own code scheme.
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
    severity: CheckSeverity | None = Field(
        default=CheckSeverity.ERROR,
        description="Severity of the check, one of 'error' or 'warn'.",
    )

    # NB: the twelve per-resource fields (``model``, ``seed``, ``catalog_node``,
    # ...) are deliberately NOT declared here. Every check binds exactly one
    # resource, and it declares that field itself: the ``@check`` decorator adds
    # it as ``Any | None``, and class-based checks must declare it too (the
    # runner infers ``iterate_over`` from the subclass's own ``__annotations__``,
    # so a subclass that declared none never matched a resource in the first
    # place). Declaring all twelve on the base made every generated subclass
    # re-collect and copy 19 inherited fields instead of 7 -- measured at ~29%
    # of total check-class construction, which is the single largest cost in a
    # warm run. ``set_resource`` writes straight into the instance ``__dict__``,
    # so nothing here needs a slot reserved for it.

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

    def _require(self, field: str) -> Any:
        """Return the named field, raising DbtBouncerFailedCheckError if it is None.

        Checks the instance first, then falls back to _ctx for global context fields.

        Args:
            field: The attribute name on this check instance (e.g. "model", "seed").

        Returns:
            The field value.

        Raises:
            DbtBouncerFailedCheckError: If the field is None.

        """
        val = getattr(self, field, None)
        if val is None and self._ctx is not None:
            val = getattr(self._ctx, field, None)
        if val is None:
            raise DbtBouncerFailedCheckError(f"self.{field} is None")
        return val

    def _require_catalog_node(self) -> CatalogNodeEntry:
        """Require catalog_node.

        Returns:
            CatalogNodeEntry: The catalog_node object.

        """
        return self._require("catalog_node")

    def _require_catalog_source(self) -> CatalogNodeEntry:
        """Require catalog_source.

        Returns:
            CatalogNodeEntry: The catalog_source object.

        """
        return self._require("catalog_source")

    def _require_exposure(self) -> ExposureNode:
        """Require exposure.

        Returns:
            ExposureNode: The exposure object.

        """
        return self._require("exposure")

    def _require_macro(self) -> MacroNode:
        """Require macro.

        Returns:
            MacroNode: The macro object.

        """
        return self._require("macro")

    def _require_manifest(self) -> ManifestWrapper:
        """Require manifest_obj.

        Returns:
            ManifestWrapper: The manifest object.

        """
        return self._require("manifest_obj")  # type: ignore[return-value]

    def _require_model(self) -> ModelNode:
        """Require model.

        Returns:
            ModelNode: The model object.

        """
        return self._require("model")

    def _require_run_result(self) -> RunResultEntry:
        """Require run_result.

        Returns:
            RunResultEntry: The run_result object.

        """
        return self._require("run_result")

    def _require_seed(self) -> SeedNode:
        """Require seed.

        Returns:
            SeedNode: The seed object.

        """
        return self._require("seed")

    def _require_semantic_model(self) -> SemanticModelNode:
        """Require semantic_model.

        Returns:
            SemanticModelNode: The semantic_model object.

        """
        return self._require("semantic_model")

    def _require_snapshot(self) -> SnapshotNode:
        """Require snapshot.

        Returns:
            SnapshotNode: The snapshot object.

        """
        return self._require("snapshot")

    def _require_source(self) -> SourceNode:
        """Require source.

        Returns:
            SourceNode: The source object.

        """
        return self._require("source")

    def _require_test(self) -> TestNode:
        """Require test.

        Returns:
            TestNode: The test object.

        """
        return self._require("test")

    def _require_unit_test(self) -> UnitTestNode:
        """Require unit_test.

        Returns:
            UnitTestNode: The unit_test object.

        """
        return self._require("unit_test")
