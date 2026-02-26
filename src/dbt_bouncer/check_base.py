from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import is_description_populated

# Cache annotation key sets per check class to avoid rebuilding on every injection call.
_ANNOTATION_KEYS_CACHE: dict[type, frozenset[str]] = {}

if TYPE_CHECKING:
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
            Nodes as CatalogNodes,
        )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
    )


class BaseCheck(BaseModel):
    """Base class for all checks."""

    model_config = ConfigDict(extra="forbid")

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
    materialization: Literal["ephemeral", "incremental", "table", "view"] | None = (
        Field(
            default=None,
            description="Limit check to models with the specified materialization.",
        )
    )
    severity: Literal["error", "warn"] | None = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )

    catalog_node: Any = Field(default=None)
    catalog_source: Any = Field(default=None)
    exposure: Any = Field(default=None)
    macro: Any = Field(default=None)
    manifest_obj: Any = Field(default=None)
    model: Any = Field(default=None)
    run_result: Any = Field(default=None)
    seed: Any = Field(default=None)
    semantic_model: Any = Field(default=None)
    snapshot: Any = Field(default=None)
    source: Any = Field(default=None)
    test: Any = Field(default=None)
    unit_test: Any = Field(default=None)

    models_by_unique_id: dict[str, Any] | None = Field(default=None)
    sources_by_unique_id: dict[str, Any] | None = Field(default=None)
    exposures_by_unique_id: dict[str, Any] | None = Field(default=None)
    tests_by_unique_id: dict[str, Any] | None = Field(default=None)

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
            object.__setattr__(
                self,
                iterate_over_value,
                getattr(resource, iterate_over_value, resource),
            )
        # Inject any global context fields that the check declares
        cls = self.__class__
        if cls not in _ANNOTATION_KEYS_CACHE:
            _ANNOTATION_KEYS_CACHE[cls] = frozenset(cls.__annotations__.keys())
        for key in parsed_data.keys() & _ANNOTATION_KEYS_CACHE[cls]:
            object.__setattr__(self, key, parsed_data[key])

    # Helper methods
    def is_catalog_node_a_model(
        self, catalog_node: "CatalogNodes", models: list["DbtBouncerModelBase"]
    ) -> bool:
        """Check if a catalog node is a model.

        Args:
            catalog_node (CatalogNodes): The CatalogNodes object to check.
            models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

        Returns:
            bool: Whether a catalog node is a model.

        """
        model = next((m for m in models if m.unique_id == catalog_node.unique_id), None)
        return model is not None and model.resource_type == "model"

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

    def _require_catalog_node(self) -> Any:
        """Require catalog_node.

        Returns:
            The catalog_node object.

        """
        return self._require("catalog_node")

    def _require_catalog_source(self) -> Any:
        """Require catalog_source.

        Returns:
            The catalog_source object.

        """
        return self._require("catalog_source")

    def _require_exposure(self) -> Any:
        """Require exposure.

        Returns:
            The exposure object.

        """
        return self._require("exposure")

    def _require_macro(self) -> Any:
        """Require macro.

        Returns:
            The macro object.

        """
        return self._require("macro")

    def _require_manifest(self) -> Any:
        """Require manifest_obj.

        Returns:
            The manifest object.

        """
        return self._require("manifest_obj")

    def _require_model(self) -> Any:
        """Require model.

        Returns:
            The model object.

        """
        return self._require("model")

    def _require_run_result(self) -> Any:
        """Require run_result.

        Returns:
            The run_result object.

        """
        return self._require("run_result")

    def _require_seed(self) -> Any:
        """Require seed.

        Returns:
            The seed object.

        """
        return self._require("seed")

    def _require_semantic_model(self) -> Any:
        """Require semantic_model.

        Returns:
            The semantic_model object.

        """
        return self._require("semantic_model")

    def _require_snapshot(self) -> Any:
        """Require snapshot.

        Returns:
            The snapshot object.

        """
        return self._require("snapshot")

    def _require_source(self) -> Any:
        """Require source.

        Returns:
            The source object.

        """
        return self._require("source")

    def _require_test(self) -> Any:
        """Require test.

        Returns:
            The test object.

        """
        return self._require("test")

    def _require_unit_test(self) -> Any:
        """Require unit_test.

        Returns:
            The unit_test object.

        """
        return self._require("unit_test")
