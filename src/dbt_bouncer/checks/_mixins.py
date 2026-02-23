"""Resource mixins for checks.

Each mixin provides a typed resource field and a _require_* method.
Compose mixins with BaseCheck when defining checks, e.g.:

    class CheckModelAccess(ModelMixin, BaseCheck):
        name: Literal["check_model_access"]
        ...
"""

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

if TYPE_CHECKING:
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
            Nodes as CatalogNodes,
        )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerExposureBase,
        DbtBouncerMacroBase,
        DbtBouncerManifest,
        DbtBouncerModelBase,
        DbtBouncerSeedBase,
        DbtBouncerSemanticModelBase,
        DbtBouncerSnapshotBase,
        DbtBouncerSourceBase,
        DbtBouncerTestBase,
    )
    from dbt_bouncer.artifact_parsers.parsers_run_results import (
        DbtBouncerRunResultBase,
    )


class CatalogNodeMixin(BaseModel):
    """Mixin for checks that iterate over catalog nodes."""

    catalog_node: "CatalogNodes | None" = Field(default=None)

    def _require_catalog_node(self) -> "CatalogNodes":
        if self.catalog_node is None:
            raise DbtBouncerFailedCheckError("self.catalog_node is None")
        return self.catalog_node


class CatalogSourceMixin(BaseModel):
    """Mixin for checks that iterate over catalog sources."""

    catalog_source: "CatalogNodes | None" = Field(default=None)

    def _require_catalog_source(self) -> "CatalogNodes":
        if self.catalog_source is None:
            raise DbtBouncerFailedCheckError("self.catalog_source is None")
        return self.catalog_source


class ExposureMixin(BaseModel):
    """Mixin for checks that iterate over exposures."""

    exposure: "DbtBouncerExposureBase | None" = Field(default=None)

    def _require_exposure(self) -> "DbtBouncerExposureBase":
        if self.exposure is None:
            raise DbtBouncerFailedCheckError("self.exposure is None")
        return self.exposure


class MacroMixin(BaseModel):
    """Mixin for checks that iterate over macros."""

    macro: "DbtBouncerMacroBase | None" = Field(default=None)

    def _require_macro(self) -> "DbtBouncerMacroBase":
        if self.macro is None:
            raise DbtBouncerFailedCheckError("self.macro is None")
        return self.macro


class ManifestMixin(BaseModel):
    """Mixin for checks that need the manifest (project-level or in addition to a resource)."""

    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)

    def _require_manifest(self) -> "DbtBouncerManifest":
        if self.manifest_obj is None:
            raise DbtBouncerFailedCheckError("self.manifest_obj is None")
        return self.manifest_obj


class ModelMixin(BaseModel):
    """Mixin for checks that iterate over models."""

    model: "DbtBouncerModelBase | None" = Field(default=None)

    def _require_model(self) -> "DbtBouncerModelBase":
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        return self.model


class RunResultMixin(BaseModel):
    """Mixin for checks that iterate over run results."""

    run_result: "DbtBouncerRunResultBase | None" = Field(default=None)

    def _require_run_result(self) -> "DbtBouncerRunResultBase":
        if self.run_result is None:
            raise DbtBouncerFailedCheckError("self.run_result is None")
        return self.run_result


class SeedMixin(BaseModel):
    """Mixin for checks that iterate over seeds."""

    seed: "DbtBouncerSeedBase | None" = Field(default=None)

    def _require_seed(self) -> "DbtBouncerSeedBase":
        if self.seed is None:
            raise DbtBouncerFailedCheckError("self.seed is None")
        return self.seed


class SemanticModelMixin(BaseModel):
    """Mixin for checks that iterate over semantic models."""

    semantic_model: "DbtBouncerSemanticModelBase | None" = Field(default=None)

    def _require_semantic_model(self) -> "DbtBouncerSemanticModelBase":
        if self.semantic_model is None:
            raise DbtBouncerFailedCheckError("self.semantic_model is None")
        return self.semantic_model


class SnapshotMixin(BaseModel):
    """Mixin for checks that iterate over snapshots."""

    snapshot: "DbtBouncerSnapshotBase | None" = Field(default=None)

    def _require_snapshot(self) -> "DbtBouncerSnapshotBase":
        if self.snapshot is None:
            raise DbtBouncerFailedCheckError("self.snapshot is None")
        return self.snapshot


class SourceMixin(BaseModel):
    """Mixin for checks that iterate over sources."""

    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def _require_source(self) -> "DbtBouncerSourceBase":
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
        return self.source


class TestMixin(BaseModel):
    """Mixin for checks that iterate over tests."""

    test: "DbtBouncerTestBase | None" = Field(default=None)

    def _require_test(self) -> "DbtBouncerTestBase":
        if self.test is None:
            raise DbtBouncerFailedCheckError("self.test is None")
        return self.test


class UnitTestMixin(BaseModel):
    """Mixin for checks that iterate over unit tests."""

    unit_test: Any = Field(default=None)  # UnitTests from manifest_latest

    def _require_unit_test(self) -> Any:
        if self.unit_test is None:
            raise DbtBouncerFailedCheckError("self.unit_test is None")
        return self.unit_test
