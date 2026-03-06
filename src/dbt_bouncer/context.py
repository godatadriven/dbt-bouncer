"""A context object to hold all the data needed for a bouncer run."""

from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import UnitTests
    from dbt_bouncer.artifact_parsers.parsers_catalog import DbtBouncerCatalogNode
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerExposureBase,
        DbtBouncerMacroBase,
        DbtBouncerManifest,
        DbtBouncerModel,
        DbtBouncerModelBase,
        DbtBouncerSeed,
        DbtBouncerSeedBase,
        DbtBouncerSemanticModel,
        DbtBouncerSemanticModelBase,
        DbtBouncerSnapshot,
        DbtBouncerSnapshotBase,
        DbtBouncerSource,
        DbtBouncerSourceBase,
        DbtBouncerTest,
        DbtBouncerTestBase,
    )
    from dbt_bouncer.artifact_parsers.parsers_run_results import (
        DbtBouncerRunResult,
        DbtBouncerRunResultBase,
    )
    from dbt_bouncer.config_file_parser import DbtBouncerConfBase


class BouncerContext(BaseModel):
    """A context object to hold all the data needed for a bouncer run."""

    model_config = ConfigDict(ignored_types=(cached_property,))
    _model_rebuilt: ClassVar[bool] = False
    bouncer_config: "DbtBouncerConfBase"
    catalog_nodes: list["DbtBouncerCatalogNode"]
    catalog_sources: list["DbtBouncerCatalogNode"]
    check_categories: list[str]
    create_pr_comment_file: bool
    dry_run: bool
    exposures: list["DbtBouncerExposureBase"]
    macros: list["DbtBouncerMacroBase"]
    manifest_obj: "DbtBouncerManifest"
    models: list["DbtBouncerModel"]
    output_file: Path | None
    output_format: str
    output_only_failures: bool
    run_results: list["DbtBouncerRunResult"]
    seeds: list["DbtBouncerSeed"]
    semantic_models: list["DbtBouncerSemanticModel"]
    show_all_failures: bool
    snapshots: list["DbtBouncerSnapshot"]
    sources: list["DbtBouncerSource"]
    tests: list["DbtBouncerTest"]
    unit_tests: list["UnitTests"]

    @cached_property
    def exposures_by_unique_id(self) -> "dict[str, DbtBouncerExposureBase]":
        """Return a dict of exposure objects keyed by unique_id."""
        return {e.unique_id: e for e in self.exposures}

    @cached_property
    def models_flat(self) -> "list[DbtBouncerModelBase]":
        """Return a list of unwrapped model objects."""
        return [m.model for m in self.models]

    @cached_property
    def models_by_unique_id(self) -> "dict[str, DbtBouncerModelBase]":
        """Return a dict of unwrapped model objects keyed by unique_id."""
        return {m.model.unique_id: m.model for m in self.models}

    @cached_property
    def run_results_flat(self) -> "list[DbtBouncerRunResultBase]":
        """Return a list of unwrapped run result objects."""
        return [r.run_result for r in self.run_results]

    @cached_property
    def seeds_flat(self) -> "list[DbtBouncerSeedBase]":
        """Return a list of unwrapped seed objects."""
        return [s.seed for s in self.seeds]

    @cached_property
    def semantic_models_flat(self) -> "list[DbtBouncerSemanticModelBase]":
        """Return a list of unwrapped semantic model objects."""
        return [s.semantic_model for s in self.semantic_models]

    @cached_property
    def snapshots_flat(self) -> "list[DbtBouncerSnapshotBase]":
        """Return a list of unwrapped snapshot objects."""
        return [s.snapshot for s in self.snapshots]

    @cached_property
    def sources_by_unique_id(self) -> "dict[str, DbtBouncerSourceBase]":
        """Return a dict of unwrapped source objects keyed by unique_id."""
        return {s.source.unique_id: s.source for s in self.sources}

    @cached_property
    def tests_by_unique_id(self) -> "dict[str, DbtBouncerTestBase]":
        """Return a dict of unwrapped test objects keyed by unique_id."""
        return {t.test.unique_id: t.test for t in self.tests}

    @cached_property
    def tests_flat(self) -> "list[DbtBouncerTestBase]":
        """Return a list of unwrapped test objects."""
        return [t.test for t in self.tests]


def _rebuild_bouncer_context() -> None:
    """Rebuild BouncerContext to resolve forward references after heavy imports."""
    if BouncerContext._model_rebuilt:
        return

    # Import the real types for model_rebuild namespace.
    # By this point, the heavy modules (manifest_latest, manifest_v11) are
    # already loaded by _import_artifact_types() in validate_conf().
    import warnings

    from dbt_bouncer.artifact_parsers.dbt_cloud import manifest_latest as _ml
    from dbt_bouncer.artifact_parsers.parsers_catalog import DbtBouncerCatalogNode
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        DbtBouncerModel,
        DbtBouncerSeed,
        DbtBouncerSemanticModel,
        DbtBouncerSnapshot,
        DbtBouncerSource,
        DbtBouncerTest,
    )
    from dbt_bouncer.artifact_parsers.parsers_run_results import (
        DbtBouncerRunResult,
        DbtBouncerRunResultBase,
    )
    from dbt_bouncer.config_file_parser import DbtBouncerConfBase

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            Exposure as Exposure_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            GenericTestNode as GenericTestNode_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            Macro as Macro_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            ModelNode as ModelNode_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            SeedNode as SeedNode_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            SemanticModel as SemanticModel_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            SingularTestNode as SingularTestNode_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            SnapshotNode as SnapshotNode_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            SourceDefinition as SourceDefinition_v11,
        )

    # Build the real TypeAlias unions for the namespace
    types_namespace = {
        "DbtBouncerCatalogNode": DbtBouncerCatalogNode,
        "DbtBouncerConfBase": DbtBouncerConfBase,
        "DbtBouncerExposureBase": Exposure_v11 | _ml.Exposures,
        "DbtBouncerMacroBase": Macro_v11 | _ml.Macros,
        "DbtBouncerManifest": DbtBouncerManifest,
        "DbtBouncerModel": DbtBouncerModel,
        "DbtBouncerModelBase": ModelNode_v11 | _ml.Nodes4 | _ml.Nodes4,
        "DbtBouncerRunResult": DbtBouncerRunResult,
        "DbtBouncerRunResultBase": DbtBouncerRunResultBase,
        "DbtBouncerSeed": DbtBouncerSeed,
        "DbtBouncerSeedBase": SeedNode_v11 | _ml.Nodes,
        "DbtBouncerSemanticModel": DbtBouncerSemanticModel,
        "DbtBouncerSemanticModelBase": SemanticModel_v11
        | _ml.SemanticModels
        | _ml.SemanticModels,
        "DbtBouncerSnapshot": DbtBouncerSnapshot,
        "DbtBouncerSnapshotBase": _ml.Nodes7 | SnapshotNode_v11,
        "DbtBouncerSource": DbtBouncerSource,
        "DbtBouncerSourceBase": SourceDefinition_v11 | _ml.Sources | _ml.Sources,
        "DbtBouncerTest": DbtBouncerTest,
        "DbtBouncerTestBase": (
            GenericTestNode_v11
            | SingularTestNode_v11
            | _ml.Nodes1
            | _ml.Nodes2
            | _ml.Nodes6
            | _ml.Nodes2
            | _ml.Nodes6
        ),
        "UnitTests": _ml.UnitTests,
    }

    BouncerContext.model_rebuild(_types_namespace=types_namespace)
    BouncerContext._model_rebuilt = True
