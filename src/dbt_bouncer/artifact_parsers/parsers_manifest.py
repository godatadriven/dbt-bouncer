import logging
import warnings
from enum import Enum
from typing import Any, TypeAlias, cast

from pydantic import BaseModel

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import ManifestLatest
from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Nodes as SeedsLatest,
)
from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Nodes2 as Nodes2Latest,
)
from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Nodes4 as Nodes4Latest,
)
from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Nodes6 as Nodes6Latest,
)
from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    SemanticModels as SemanticModelsLatest,
)
from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Sources as SourcesLatest,
)
from dbt_bouncer.utils import clean_path_str, get_package_version_number

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
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import ManifestV11
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
    Nodes1,
    Nodes2,
    Nodes4,
    Nodes6,
    Nodes7,
    SemanticModels,
    Sources,
    UnitTests,
)


class DbtBouncerManifest(BaseModel):
    """Model for all manifest objects."""

    manifest: ManifestV11 | ManifestLatest


DbtBouncerExposureBase: TypeAlias = Exposure_v11 | Exposures


class DbtBouncerExposure(BaseModel):
    """Model for all exposure nodes in `manifest.json`."""

    model: DbtBouncerExposureBase
    original_file_path: str
    unique_id: str


DbtBouncerMacroBase: TypeAlias = Macro_v11 | Macros


DbtBouncerModelBase: TypeAlias = ModelNode_v11 | Nodes4 | Nodes4Latest


class DbtBouncerModel(BaseModel):
    """Model for all model nodes in `manifest.json`."""

    model: DbtBouncerModelBase
    original_file_path: str
    unique_id: str


DbtBouncerSeedBase: TypeAlias = SeedNode_v11 | SeedsLatest


class DbtBouncerSeed(BaseModel):
    """Model for all seed nodes in `manifest.json`."""

    original_file_path: str
    seed: DbtBouncerSeedBase
    unique_id: str


DbtBouncerSemanticModelBase: TypeAlias = (
    SemanticModel_v11 | SemanticModels | SemanticModelsLatest
)


class DbtBouncerSemanticModel(BaseModel):
    """Model for all semantic model nodes in `manifest.json`."""

    original_file_path: str
    semantic_model: DbtBouncerSemanticModelBase
    unique_id: str


DbtBouncerSnapshotBase: TypeAlias = Nodes7 | SnapshotNode_v11


class DbtBouncerSnapshot(BaseModel):
    """Model for all snapshot nodes in `manifest.json`."""

    original_file_path: str
    snapshot: DbtBouncerSnapshotBase
    unique_id: str


DbtBouncerSourceBase: TypeAlias = SourceDefinition_v11 | Sources | SourcesLatest


class DbtBouncerSource(BaseModel):
    """Model for all source nodes in `manifest.json`."""

    original_file_path: str
    source: DbtBouncerSourceBase
    unique_id: str


DbtBouncerTestBase: TypeAlias = (
    GenericTestNode_v11
    | SingularTestNode_v11
    | Nodes1
    | Nodes2
    | Nodes6
    | Nodes2Latest
    | Nodes6Latest
)


class DbtBouncerTest(BaseModel):
    """Model for all test nodes in `manifest.json`."""

    original_file_path: str
    test: DbtBouncerTestBase
    unique_id: str


def parse_manifest(
    manifest: dict[str, Any],
) -> ManifestV11 | ManifestLatest:
    """Parse manifest.json.

    Args:
        manifest: A dict of manifest.json

    Raises:
        ValueError: If the manifest.json is not a valid manifest.json

    Returns:
       ManifestV11 | ManifestLatest

    """
    dbt_schema_version = manifest["metadata"]["dbt_schema_version"]
    if dbt_schema_version == "https://schemas.getdbt.com/dbt/manifest/v11.json":
        return ManifestV11(**manifest)
    elif dbt_schema_version == "https://schemas.getdbt.com/dbt/manifest/v12.json":
        from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
            ManifestLatest,
        )

        return ManifestLatest(**manifest)

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
    for k, v in manifest_obj.manifest.nodes.items():
        if (
            isinstance(v.resource_type, Enum) and v.resource_type.value == "model"
        ) or v.resource_type == "model":
            if v.package_name == (
                package_name or manifest_obj.manifest.metadata.project_name
            ):
                project_models.append(
                    DbtBouncerModel(
                        model=cast("Any", v),
                        original_file_path=str(clean_path_str(v.original_file_path)),
                        unique_id=k,
                    ),
                )
        elif (
            (isinstance(v.resource_type, Enum) and v.resource_type.value == "seed")
            or v.resource_type == "seed"
        ) and v.package_name == (
            package_name or manifest_obj.manifest.metadata.project_name
        ):
            project_seeds.append(
                DbtBouncerSeed(
                    original_file_path=str(clean_path_str(v.original_file_path)),
                    seed=cast("Any", v),
                    unique_id=k,
                ),
            )
        elif (
            (isinstance(v.resource_type, Enum) and v.resource_type.value == "snapshot")
            or v.resource_type == "snapshot"
        ) and v.package_name == (
            package_name or manifest_obj.manifest.metadata.project_name
        ):
            project_snapshots.append(
                DbtBouncerSnapshot(
                    original_file_path=str(clean_path_str(v.original_file_path)),
                    snapshot=cast("Any", v),
                    unique_id=k,
                ),
            )
        elif (
            (isinstance(v.resource_type, Enum) and v.resource_type.value == "test")
            or v.resource_type == "test"
        ) and v.package_name == (
            package_name or manifest_obj.manifest.metadata.project_name
        ):
            project_tests.append(
                DbtBouncerTest(
                    original_file_path=str(clean_path_str(v.original_file_path)),
                    test=cast("Any", v),
                    unique_id=k,
                ),
            )

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

    project_semantic_models = [
        DbtBouncerSemanticModel(
            original_file_path=str(clean_path_str(v.original_file_path)),
            semantic_model=cast("Any", v),
            unique_id=k,
        )
        for k, v in manifest_obj.manifest.semantic_models.items()
        if v.package_name
        == (package_name or manifest_obj.manifest.metadata.project_name)
    ]

    project_sources = [
        DbtBouncerSource(
            original_file_path=str(clean_path_str(v.original_file_path)),
            source=cast("Any", v),
            unique_id=k,
        )
        for k, v in manifest_obj.manifest.sources.items()
        if v.package_name
        == (package_name or manifest_obj.manifest.metadata.project_name)
    ]

    logging.info(
        f"Parsed `manifest.json`, found `{(package_name or manifest_obj.manifest.metadata.project_name)}` project: {len(project_exposures)} exposures, {len(project_macros)} macros, {len(project_models)} nodes, {len(project_seeds)} seeds, {len(project_semantic_models)} semantic models, {len(project_snapshots)} snapshots, {len(project_sources)} sources, {len(project_tests)} tests, {len(project_unit_tests)} unit tests.",
    )
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
