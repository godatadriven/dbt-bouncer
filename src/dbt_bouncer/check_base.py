from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from dbt_bouncer.artifact_types import (  # noqa: TC001 - needed at runtime for Pydantic model_rebuild
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
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.enums import CheckSeverity, Materialization
from dbt_bouncer.utils import is_description_populated


class BaseCheck(BaseModel):
    """Base class for all checks."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    description: str | None = Field(
        default=None,
        description="Description of what the check does and why it is implemented.",
    )
    exclude: str | None = Field(
        default=None,
        description="Regexp to match which paths to exclude.",
    )
    include: str | None = Field(
        default=None,
        description="Regexp to match which paths to include.",
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

    catalog_node: CatalogNodeEntry | None = Field(default=None)
    catalog_source: CatalogNodeEntry | None = Field(default=None)
    exposure: ExposureNode | None = Field(default=None)
    macro: MacroNode | None = Field(default=None)
    model: ModelNode | None = Field(default=None)
    run_result: RunResultEntry | None = Field(default=None)
    seed: SeedNode | None = Field(default=None)
    semantic_model: SemanticModelNode | None = Field(default=None)
    snapshot: SnapshotNode | None = Field(default=None)
    source: SourceNode | None = Field(default=None)
    test: TestNode | None = Field(default=None)
    unit_test: UnitTestNode | None = Field(default=None)

    _ctx: Any = PrivateAttr(default=None)
    _min_description_length: ClassVar[int] = 4

    def set_resource(self, resource: Any, iterate_over_value: str) -> None:
        """Set the per-iteration resource on this check instance.

        Args:
            resource: The dbt resource wrapper object (e.g. DbtBouncerModel).
            iterate_over_value: The field name to set (e.g. "model", "seed").

        """
        object.__setattr__(
            self,
            iterate_over_value,
            getattr(resource, iterate_over_value, resource),
        )

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
