from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.artifact_types import (  # noqa: TC001 - needed at runtime for Pydantic model_rebuild
    CatalogNodeEntry,
    ExposureNode,
    MacroNode,
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

# Cache annotation key sets per check class to avoid rebuilding on every injection call.
_ANNOTATION_KEYS_CACHE: dict[type, frozenset[str]] = {}


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
    manifest_obj: Any | None = Field(default=None)
    model: ModelNode | None = Field(default=None)
    run_result: RunResultEntry | None = Field(default=None)
    seed: SeedNode | None = Field(default=None)
    semantic_model: SemanticModelNode | None = Field(default=None)
    snapshot: SnapshotNode | None = Field(default=None)
    source: SourceNode | None = Field(default=None)
    test: TestNode | None = Field(default=None)
    unit_test: UnitTestNode | None = Field(default=None)

    models_by_unique_id: dict[str, ModelNode] | None = Field(default=None)
    sources_by_unique_id: dict[str, SourceNode] | None = Field(default=None)
    exposures_by_unique_id: dict[str, ExposureNode] | None = Field(default=None)
    tests_by_unique_id: dict[str, TestNode] | None = Field(default=None)

    _min_description_length: ClassVar[int] = 4

    def _inject_context(
        self,
        parsed_data: dict[str, Any],
        resource: Any = None,
        iterate_over_value: str | None = None,
    ) -> None:
        """Inject a resource and global context data into this check instance.

        This replaces the ad-hoc setattr calls in runner.py with a single,
        self-documenting method on the check base class.

        Args:
            parsed_data: Dict of global context keys (manifest, catalog_nodes, etc.) to inject.
            resource: The dbt resource object to inject (e.g. a DbtBouncerModel wrapper).
                      When None, only global context is injected (for non-iterating checks).
            iterate_over_value: The annotation key that names the resource field (e.g. "model").

        """
        # Inject the specific resource into the matching field (only for iterating checks)
        if resource is not None and iterate_over_value is not None:
            # Wrapped resources (SimpleNamespace) have the inner attribute (e.g. .model);
            # unwrapped resources (bare DictProxy for exposures/macros/unit_tests) don't.
            # Use hasattr on SimpleNamespace or dict-key check on DictProxy.
            if isinstance(resource, dict):
                inner = resource.get(iterate_over_value, resource)
            else:
                inner = getattr(resource, iterate_over_value, resource)
            object.__setattr__(self, iterate_over_value, inner)
        # Inject any global context fields that the check declares
        cls = self.__class__
        if cls not in _ANNOTATION_KEYS_CACHE:
            _ANNOTATION_KEYS_CACHE[cls] = frozenset(cls.__annotations__.keys())
        for key in parsed_data.keys() & _ANNOTATION_KEYS_CACHE[cls]:
            object.__setattr__(self, key, parsed_data[key])

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

        Args:
            field: The attribute name on this check instance (e.g. "model", "seed").

        Returns:
            The field value.

        Raises:
            DbtBouncerFailedCheckError: If the field is None.

        """
        val = getattr(self, field, None)
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

    def _require_manifest(self) -> Any:
        """Require manifest_obj.

        Returns:
            Any: The manifest object.

        """
        return self._require("manifest_obj")

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
