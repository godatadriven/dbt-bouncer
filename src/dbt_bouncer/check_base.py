import logging
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import (
    compile_pattern,
    get_package_version_number,
    is_description_populated,
)

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

    def _require_model(self) -> Any:
        """Require that the model field is not None.

        Returns:
            The model object.

        Raises:
            DbtBouncerFailedCheckError: If model is None.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        return self.model

    def _require_seed(self) -> Any:
        """Require that the seed field is not None.

        Returns:
            The seed object.

        Raises:
            DbtBouncerFailedCheckError: If seed is None.

        """
        if self.seed is None:
            raise DbtBouncerFailedCheckError("self.seed is None")
        return self.seed

    def _require_snapshot(self) -> Any:
        """Require that the snapshot field is not None.

        Returns:
            The snapshot object.

        Raises:
            DbtBouncerFailedCheckError: If snapshot is None.

        """
        if self.snapshot is None:
            raise DbtBouncerFailedCheckError("self.snapshot is None")
        return self.snapshot

    def _require_source(self) -> Any:
        """Require that the source field is not None.

        Returns:
            The source object.

        Raises:
            DbtBouncerFailedCheckError: If source is None.

        """
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
        return self.source

    def _require_test(self) -> Any:
        """Require that the test field is not None.

        Returns:
            The test object.

        Raises:
            DbtBouncerFailedCheckError: If test is None.

        """
        if self.test is None:
            raise DbtBouncerFailedCheckError("self.test is None")
        return self.test

    def _require_exposure(self) -> Any:
        """Require that the exposure field is not None.

        Returns:
            The exposure object.

        Raises:
            DbtBouncerFailedCheckError: If exposure is None.

        """
        if self.exposure is None:
            raise DbtBouncerFailedCheckError("self.exposure is None")
        return self.exposure

    def _require_macro(self) -> Any:
        """Require that the macro field is not None.

        Returns:
            The macro object.

        Raises:
            DbtBouncerFailedCheckError: If macro is None.

        """
        if self.macro is None:
            raise DbtBouncerFailedCheckError("self.macro is None")
        return self.macro

    def _require_catalog_node(self) -> Any:
        """Require that the catalog_node field is not None.

        Returns:
            The catalog_node object.

        Raises:
            DbtBouncerFailedCheckError: If catalog_node is None.

        """
        if self.catalog_node is None:
            raise DbtBouncerFailedCheckError("self.catalog_node is None")
        return self.catalog_node

    def _require_catalog_source(self) -> Any:
        """Require that the catalog_source field is not None.

        Returns:
            The catalog_source object.

        Raises:
            DbtBouncerFailedCheckError: If catalog_source is None.

        """
        if self.catalog_source is None:
            raise DbtBouncerFailedCheckError("self.catalog_source is None")
        return self.catalog_source

    def _require_run_result(self) -> Any:
        """Require that the run_result field is not None.

        Returns:
            The run_result object.

        Raises:
            DbtBouncerFailedCheckError: If run_result is None.

        """
        if self.run_result is None:
            raise DbtBouncerFailedCheckError("self.run_result is None")
        return self.run_result

    def _require_manifest(self) -> Any:
        """Require that the manifest_obj field is not None.

        Returns:
            The manifest object.

        Raises:
            DbtBouncerFailedCheckError: If manifest_obj is None.

        """
        if self.manifest_obj is None:
            raise DbtBouncerFailedCheckError("self.manifest_obj is None")
        return self.manifest_obj

    def _require_semantic_model(self) -> Any:
        """Require that the semantic_model field is not None.

        Returns:
            The semantic_model object.

        Raises:
            DbtBouncerFailedCheckError: If semantic_model is None.

        """
        if self.semantic_model is None:
            raise DbtBouncerFailedCheckError("self.semantic_model is None")
        return self.semantic_model

    def _require_unit_test(self) -> Any:
        """Require that the unit_test field is not None.

        Returns:
            The unit_test object.

        Raises:
            DbtBouncerFailedCheckError: If unit_test is None.

        """
        if self.unit_test is None:
            raise DbtBouncerFailedCheckError("self.unit_test is None")
        return self.unit_test


class BaseNamePatternCheck(ABC, BaseCheck):
    """Abstract base for checks that validate a resource name against a regex pattern.

    Subclasses must define: _name_pattern (str), _resource_name (str), _resource_display_name (str).
    """

    _compiled_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_pattern = compile_pattern(self._name_pattern.strip())

    def execute(self) -> None:
        """Execute the check: require name to match the pattern.

        Raises:
            DbtBouncerFailedCheckError: If name does not match the pattern.

        """
        if self._compiled_pattern.match(self._resource_name) is None:
            raise DbtBouncerFailedCheckError(
                f"`{self._resource_display_name}` does not match the supplied regex `{self._name_pattern.strip()}`."
            )

    @property
    @abstractmethod
    def _name_pattern(self) -> str: ...

    @property
    @abstractmethod
    def _resource_name(self) -> str: ...

    @property
    @abstractmethod
    def _resource_display_name(self) -> str: ...


class BaseDescriptionPopulatedCheck(ABC, BaseCheck):
    """Abstract base for checks that require a populated description.

    Subclasses must define: _resource_description (str), _resource_display_name (str).
    """

    min_description_length: int | None = Field(default=None)

    def execute(self) -> None:
        """Execute the check: require description to be populated.

        Raises:
            DbtBouncerFailedCheckError: If description is not populated.

        """
        if not self._is_description_populated(
            self._resource_description, self.min_description_length
        ):
            raise DbtBouncerFailedCheckError(
                f"`{self._resource_display_name}` does not have a populated description."
            )

    @property
    @abstractmethod
    def _resource_description(self) -> str: ...

    @property
    @abstractmethod
    def _resource_display_name(self) -> str: ...


class BaseColumnsHaveTypesCheck(ABC, BaseCheck):
    """Abstract base for checks that require all columns to have a data_type.

    Subclasses must define: _resource_columns (dict-like with .data_type on values), _resource_display_name (str).
    """

    def execute(self) -> None:
        """Execute the check: require all columns to have a declared data_type.

        Raises:
            DbtBouncerFailedCheckError: If any column lacks a declared data_type.

        """
        columns = self._resource_columns
        untyped_columns = [
            col_name for col_name, col in columns.items() if not col.data_type
        ]
        if untyped_columns:
            raise DbtBouncerFailedCheckError(
                f"`{self._resource_display_name}` has columns without a declared `data_type`: {untyped_columns}"
            )

    @property
    @abstractmethod
    def _resource_columns(self) -> dict[str, Any]: ...

    @property
    @abstractmethod
    def _resource_display_name(self) -> str: ...


class BaseHasUnitTestsCheck(ABC, BaseCheck):
    """Abstract base for checks that require a minimum number of unit tests (dbt 1.8+).

    Subclasses must define: _resource_unique_id (str), _resource_display_name (str).
    """

    manifest_obj: Any = Field(default=None)
    min_number_of_unit_tests: int = Field(default=1)
    unit_tests: list[Any] = Field(default=[])

    def execute(self) -> None:
        """Execute the check: require at least min_number_of_unit_tests for the resource.

        Raises:
            DbtBouncerFailedCheckError: If unit test count is below the minimum.

        """
        self._require_manifest()
        if get_package_version_number(
            self.manifest_obj.manifest.metadata.dbt_version or "0.0.0"
        ) >= get_package_version_number("1.8.0"):
            num_unit_tests = len(
                [
                    t.unique_id
                    for t in self.unit_tests
                    if t.depends_on
                    and t.depends_on.nodes
                    and t.depends_on.nodes[0] == self._resource_unique_id
                ],
            )
            if num_unit_tests < self.min_number_of_unit_tests:
                raise DbtBouncerFailedCheckError(
                    f"`{self._resource_display_name}` has {num_unit_tests} unit tests, this is less than the minimum of {self.min_number_of_unit_tests}."
                )
        else:
            logging.warning(
                "The unit tests check is only supported for dbt 1.8.0 and above.",
            )

    @property
    @abstractmethod
    def _resource_unique_id(self) -> str: ...

    @property
    @abstractmethod
    def _resource_display_name(self) -> str: ...
