from __future__ import annotations

import warnings
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeAlias

from pydantic import BaseModel

from dbt_bouncer.utils import clean_path_str, get_package_version_number

if TYPE_CHECKING:
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

    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
        Exposures,
        Macros,
        Nodes,
        Nodes1,
        Nodes2,
        Nodes4,
        Nodes6,
        Nodes7,
        SemanticModels,
        Sources,
        UnitTests,
    )

    DbtBouncerExposureBase: TypeAlias = Exposure_v11 | Exposures
    DbtBouncerMacroBase: TypeAlias = Macro_v11 | Macros
    DbtBouncerModelBase: TypeAlias = ModelNode_v11 | Nodes4 | Nodes4
    DbtBouncerSeedBase: TypeAlias = SeedNode_v11 | Nodes
    DbtBouncerSemanticModelBase: TypeAlias = (
        SemanticModel_v11 | SemanticModels | SemanticModels
    )
    DbtBouncerSnapshotBase: TypeAlias = Nodes7 | SnapshotNode_v11
    DbtBouncerSourceBase: TypeAlias = SourceDefinition_v11 | Sources | Sources
    DbtBouncerTestBase: TypeAlias = (
        GenericTestNode_v11
        | SingularTestNode_v11
        | Nodes1
        | Nodes2
        | Nodes6
        | Nodes2
        | Nodes6
    )

# Lazy loading via PEP 562.  The TypeAlias unions and their constituent
# types are imported on first access, deferring the expensive
# manifest_latest.py and manifest_v11 loads until actually needed.
_LAZY_NAMES: set[str] = {
    "DbtBouncerExposureBase",
    "DbtBouncerMacroBase",
    "DbtBouncerModelBase",
    "DbtBouncerSeedBase",
    "DbtBouncerSemanticModelBase",
    "DbtBouncerSnapshotBase",
    "DbtBouncerSourceBase",
    "DbtBouncerTestBase",
}

_loaded = False


def _ensure_loaded() -> None:
    global _loaded
    if _loaded:
        return
    _loaded = True

    import dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest as _ml

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            Exposure as _Exposure_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            GenericTestNode as _GenericTestNode_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            Macro as _Macro_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            ModelNode as _ModelNode_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            SeedNode as _SeedNode_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            SemanticModel as _SemanticModel_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            SingularTestNode as _SingularTestNode_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            SnapshotNode as _SnapshotNode_v11,
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
            SourceDefinition as _SourceDefinition_v11,
        )

    g = globals()
    g["DbtBouncerExposureBase"] = _Exposure_v11 | _ml.Exposures
    g["DbtBouncerMacroBase"] = _Macro_v11 | _ml.Macros
    g["DbtBouncerModelBase"] = _ModelNode_v11 | _ml.Nodes4 | _ml.Nodes4
    g["DbtBouncerSeedBase"] = _SeedNode_v11 | _ml.Nodes
    g["DbtBouncerSemanticModelBase"] = (
        _SemanticModel_v11 | _ml.SemanticModels | _ml.SemanticModels
    )
    g["DbtBouncerSnapshotBase"] = _ml.Nodes7 | _SnapshotNode_v11
    g["DbtBouncerSourceBase"] = _SourceDefinition_v11 | _ml.Sources | _ml.Sources
    g["DbtBouncerTestBase"] = (
        _GenericTestNode_v11
        | _SingularTestNode_v11
        | _ml.Nodes1
        | _ml.Nodes2
        | _ml.Nodes6
        | _ml.Nodes2
        | _ml.Nodes6
    )


def __getattr__(name: str) -> Any:
    if name in _LAZY_NAMES:
        _ensure_loaded()
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class DbtBouncerManifest(BaseModel):
    """Model for all manifest objects."""

    manifest: Any  # ManifestV11 | ManifestLatest, kept as Any for flexibility


class DbtBouncerExposure(BaseModel):
    """Model for all exposure nodes in `manifest.json`."""

    model: DbtBouncerExposureBase
    original_file_path: str
    unique_id: str


class DbtBouncerModel(BaseModel):
    """Model for all model nodes in `manifest.json`."""

    model: DbtBouncerModelBase
    original_file_path: str
    unique_id: str


class DbtBouncerSeed(BaseModel):
    """Model for all seed nodes in `manifest.json`."""

    original_file_path: str
    seed: DbtBouncerSeedBase
    unique_id: str


class DbtBouncerSemanticModel(BaseModel):
    """Model for all semantic model nodes in `manifest.json`."""

    original_file_path: str
    semantic_model: DbtBouncerSemanticModelBase
    unique_id: str


class DbtBouncerSnapshot(BaseModel):
    """Model for all snapshot nodes in `manifest.json`."""

    original_file_path: str
    snapshot: DbtBouncerSnapshotBase
    unique_id: str


class DbtBouncerSource(BaseModel):
    """Model for all source nodes in `manifest.json`."""

    original_file_path: str
    source: DbtBouncerSourceBase
    unique_id: str


class DbtBouncerTest(BaseModel):
    """Model for all test nodes in `manifest.json`."""

    original_file_path: str
    test: DbtBouncerTestBase
    unique_id: str


def parse_manifest(
    manifest: dict[str, Any],
) -> Any:
    """Parse manifest.json.

    Args:
        manifest: A dict of manifest.json

    Raises:
        ValueError: If the manifest.json is not a valid manifest.json

    Returns:
       ManifestLatest or ManifestV11 depending on schema version

    """
    dbt_schema_version = manifest["metadata"]["dbt_schema_version"]
    match dbt_schema_version:
        case "https://schemas.getdbt.com/dbt/manifest/v11.json":
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning)
                from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
                    ManifestV11,
                )
            return ManifestV11(**manifest)
        case "https://schemas.getdbt.com/dbt/manifest/v12.json":
            from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
                ManifestLatest,
            )

            return ManifestLatest(**manifest)
        case _:
            raise ValueError("Not a manifest.json")


def parse_manifest_artifact(
    manifest_obj: DbtBouncerManifest, package_name: str | None = None
) -> tuple[
    list[DbtBouncerExposureBase],
    list[DbtBouncerMacroBase],
    list[DbtBouncerModel],
    list[DbtBouncerSeed],
    list[DbtBouncerSemanticModel],
    list[DbtBouncerSnapshot],
    list[DbtBouncerSource],
    list[DbtBouncerTest],
    list[UnitTests],
]:
    """Parse the manifest.json artifact.

    Returns:
        list[DbtBouncerExposure]: List of exposures in the project.
        list[DbtBouncerMacro]: List of macros in the project.
        list[DbtBouncerModel]: List of models in the project.
        list[DbtBouncerSeed]: List of seeds in the project.
        list[DbtBouncerSemanticModel]: List of semantic models in the project.
        list[DbtBouncerSnapshot]: List of snapshots in the project.
        list[DbtBouncerSource]: List of sources in the project.
        list[DbtBouncerTest]: List of tests in the project.
        list[DbtBouncerUnitTest]: List of unit tests in the project.

    """
    _ensure_loaded()

    project_exposures = [
        v
        for _, v in manifest_obj.manifest.exposures.items()
        if v.package_name
        == (package_name or manifest_obj.manifest.metadata.project_name)
    ]
    project_macros = [
        v
        for _, v in manifest_obj.manifest.macros.items()
        if v.package_name
        == (package_name or manifest_obj.manifest.metadata.project_name)
    ]
    project_models = []
    project_seeds = []
    project_snapshots = []
    project_tests = []
    target_package = package_name or manifest_obj.manifest.metadata.project_name
    for k, v in manifest_obj.manifest.nodes.items():
        resource_type_value = (
            v.resource_type.value
            if isinstance(v.resource_type, Enum)
            else v.resource_type
        )
        match resource_type_value:
            case "model" if v.package_name == target_package:
                project_models.append(
                    DbtBouncerModel(
                        model=v,
                        original_file_path=str(clean_path_str(v.original_file_path)),
                        unique_id=k,
                    ),
                )
            case "seed" if v.package_name == target_package:
                project_seeds.append(
                    DbtBouncerSeed(
                        original_file_path=str(clean_path_str(v.original_file_path)),
                        seed=v,
                        unique_id=k,
                    ),
                )
            case "snapshot" if v.package_name == target_package:
                project_snapshots.append(
                    DbtBouncerSnapshot(
                        original_file_path=str(clean_path_str(v.original_file_path)),
                        snapshot=v,
                        unique_id=k,
                    ),
                )
            case "test" if v.package_name == target_package:
                project_tests.append(
                    DbtBouncerTest(
                        original_file_path=str(clean_path_str(v.original_file_path)),
                        test=v,
                        unique_id=k,
                    ),
                )

    project_semantic_models = [
        DbtBouncerSemanticModel(
            original_file_path=str(clean_path_str(v.original_file_path)),
            semantic_model=v,
            unique_id=k,
        )
        for k, v in manifest_obj.manifest.semantic_models.items()
        if v.package_name
        == (package_name or manifest_obj.manifest.metadata.project_name)
    ]
    project_sources = [
        DbtBouncerSource(
            original_file_path=str(clean_path_str(v.original_file_path)),
            source=v,
            unique_id=k,
        )
        for k, v in manifest_obj.manifest.sources.items()
        if v.package_name
        == (package_name or manifest_obj.manifest.metadata.project_name)
    ]

    if get_package_version_number(
        manifest_obj.manifest.metadata.dbt_version or "0.0.0"
    ) >= get_package_version_number("1.8.0"):
        project_unit_tests = [
            v
            for _, v in getattr(manifest_obj.manifest, "unit_tests", {}).items()
            if v.package_name
            == (package_name or manifest_obj.manifest.metadata.project_name)
        ]
    else:
        project_unit_tests = []

    return (
        project_exposures,
        project_macros,
        project_models,
        project_seeds,
        project_semantic_models,
        project_snapshots,
        project_sources,
        project_tests,
        project_unit_tests,
    )
