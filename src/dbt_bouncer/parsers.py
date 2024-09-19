# TODO Remove after this program no longer support Python 3.8.*
from __future__ import annotations

import json
import logging
import warnings
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

import semver
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated

from dbt_bouncer.utils import clean_path_str

if TYPE_CHECKING:
    from dbt_bouncer.config_file_validator import DbtBouncerConf

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.base import BaseParserModel
    from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogTable, CatalogV1
    from dbt_artifacts_parser.parsers.manifest.manifest_v10 import (
        Exposure as Exposure_v10,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v10 import (
        GenericTestNode as GenericTestNode_v10,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v10 import ManifestV10
    from dbt_artifacts_parser.parsers.manifest.manifest_v10 import (
        ModelNode as ModelNode_v10,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v10 import (
        SemanticModel as SemanticModel_v10,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v10 import (
        SingularTestNode as SingularTestNode_v10,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v10 import (
        SourceDefinition as SourceDefinition_v10,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
        Exposure as Exposure_v11,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
        GenericTestNode as GenericTestNode_v11,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import ManifestV11
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
        ModelNode as ModelNode_v11,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
        SemanticModel as SemanticModel_v11,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
        SingularTestNode as SingularTestNode_v11,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
        SourceDefinition as SourceDefinition_v11,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (
        Access,
        Checksum,
        Columns2,
        Columns4,
        Config3,
        Config5,
        Constraint5,
        Contract8,
        DeferRelation1,
        Disabled,
        Disabled1,
        Disabled2,
        Disabled3,
        Disabled4,
        Disabled5,
        Disabled6,
        Disabled7,
        Disabled8,
        Disabled9,
        Disabled10,
        Disabled11,
        Disabled12,
        Disabled13,
        Docs,
        Docs18,
        Exposures,
        ExtraCte,
        Groups,
        Macros,
        Metadata,
        Metrics,
        Nodes,
        Nodes2,
        Nodes4,
        Nodes6,
        Ref,
        SavedQueries,
        SemanticModels,
        Sources,
        TestMetadata,
        UnitTests,
    )
    from dbt_artifacts_parser.parsers.run_results.run_results_v4 import (
        RunResultOutput as RunResultOutput_v4,
    )
    from dbt_artifacts_parser.parsers.run_results.run_results_v4 import RunResultsV4
    from dbt_artifacts_parser.parsers.run_results.run_results_v5 import (
        RunResultOutput as RunResultOutput_v5,
    )
    from dbt_artifacts_parser.parsers.run_results.run_results_v5 import RunResultsV5
    from dbt_artifacts_parser.parsers.run_results.run_results_v6 import (
        Result,
        RunResultsV6,
    )
    from dbt_artifacts_parser.parsers.utils import get_dbt_schema_version
    from dbt_artifacts_parser.parsers.version_map import ArtifactTypes


with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)

    class DbtBouncerNodes(BaseParserModel):  # noqa: D101
        model_config = ConfigDict(
            extra="allow",
        )
        database: Optional[str] = None
        schema: str
        name: str
        package_name: str
        path: str
        original_file_path: str
        unique_id: str
        fqn: List[str]
        alias: str
        checksum: Checksum = Field(..., title="FileHash")
        config: Optional[Union[Config3, Config5]] = None
        tags: Optional[List[str]] = None
        description: Optional[str] = ""
        columns: Optional[Dict[str, Union[Columns2, Columns4]]] = None
        meta: Optional[Dict[str, Any]] = None
        group: Optional[str] = None
        docs: Optional[Docs] = Field(None, title="Docs")
        patch_path: Optional[str] = None
        build_path: Optional[str] = None
        unrendered_config: Optional[Dict[str, Any]] = None
        created_at: Optional[float] = None
        config_call_dict: Optional[Dict[str, Any]] = None
        relation_name: Optional[str] = None
        raw_code: Optional[str] = ""
        language: Optional[str] = "sql"
        refs: Optional[List[Ref]] = None
        sources: Optional[List[List[str]]] = None
        metrics: Optional[List[List[str]]] = None
        depends_on: Optional[DependsOn1] = Field(None, title="DependsOn")
        compiled_path: Optional[str] = None
        compiled: Optional[bool] = False
        compiled_code: Optional[str] = None
        extra_ctes_injected: Optional[bool] = False
        extra_ctes: Optional[List[ExtraCte]] = None
        field_pre_injected_sql: Optional[str] = Field(None, alias="_pre_injected_sql")
        contract: Optional[Contract8] = Field(None, title="Contract")

    class DbtBouncerNodesModel(DbtBouncerNodes):  # noqa: D101
        model_config = ConfigDict(
            extra="allow",
        )
        access: Optional[Access] = "protected"
        constraints: Optional[List[Constraint5]] = None
        defer_relation: Optional[DeferRelation1] = None
        deprecation_date: Optional[str] = None
        latest_version: Optional[Union[str, float]] = None
        primary_key: Optional[List[str]] = None
        resource_type: Literal["model"]
        time_spine: Optional[TimeSpine] = None
        version: Optional[Union[str, float]] = None

    class DbtBouncerNodesSeed(DbtBouncerNodes):  # noqa: D101
        model_config = ConfigDict(
            extra="allow",
        )
        resource_type: Literal["seed"]

    class DbtBouncerNodesTest(DbtBouncerNodes):  # noqa: D101
        model_config = ConfigDict(
            extra="allow",
        )
        attached_node: Optional[str] = None
        column_name: Optional[str] = None
        resource_type: Literal["test"]
        test_metadata: Optional[TestMetadata] = Field(None, title="TestMetadata")


class DbtBouncerCatalogNode(BaseModel):
    """Model for all nodes in `catalog.json`."""

    catalog_node: CatalogTable
    original_file_path: str
    unique_id: str


class DbtBouncerManifest(BaseModel):
    """Model for all manifest objects."""

    manifest: Union[ManifestV10, ManifestV11, DbtBouncerManifestV12]


DbtBouncerExposureBase = Union[Exposure_v10, Exposure_v11, Exposures]


class DbtBouncerExposure(BaseModel):
    """Model for all exposure nodes in `manifest.json`."""

    model: DbtBouncerExposureBase
    original_file_path: str
    unique_id: str


class DependsOn1(BaseParserModel):  # noqa: D101
    model_config = ConfigDict(
        extra="forbid",
    )
    macros: Optional[List[str]] = None
    nodes: Optional[List[str]] = None


class TimeSpine(BaseParserModel):  # noqa: D101
    model_config = ConfigDict(
        extra="forbid",
    )
    standard_granularity_column: str


DbtBouncerModelBase = Union[ModelNode_v10, ModelNode_v11, DbtBouncerNodesModel]


class DbtBouncerModel(BaseModel):
    """Model for all model nodes in `manifest.json`."""

    model: DbtBouncerModelBase
    original_file_path: str
    unique_id: str


DbtBouncerRunResultBase = Union[RunResultOutput_v4, RunResultOutput_v5, Result]


class DbtBouncerRunResult(BaseModel):
    """Model for all results in `run_results.json`."""

    original_file_path: str
    run_result: DbtBouncerRunResultBase
    unique_id: str


DbtBouncerSemanticModelBase = Union[
    SemanticModel_v10, SemanticModel_v11, SemanticModels
]


class DbtBouncerSemanticModel(BaseModel):
    """Model for all semantic model nodes in `manifest.json`."""

    original_file_path: str
    semantic_model: DbtBouncerSemanticModelBase
    unique_id: str


DbtBouncerSourceBase = Union[SourceDefinition_v10, SourceDefinition_v11, Sources]


class DbtBouncerSource(BaseModel):
    """Model for all source nodes in `manifest.json`."""

    original_file_path: str
    source: DbtBouncerSourceBase
    unique_id: str


DbtBouncerTestBase = Union[
    GenericTestNode_v10,
    SingularTestNode_v10,
    GenericTestNode_v11,
    SingularTestNode_v11,
    DbtBouncerNodesTest,
    Nodes6,
]


class DbtBouncerTest(BaseModel):
    """Model for all test nodes in `manifest.json`."""

    original_file_path: str
    test: DbtBouncerTestBase
    unique_id: str


def load_dbt_artifact(
    artifact_name: Literal["catalog.json", "manifest.json", "run_results.json"],
    dbt_artifacts_dir: Path,
) -> Union[CatalogV1, DbtBouncerManifest, RunResultsV4, RunResultsV5, RunResultsV6]:
    """Load a dbt artifact from a JSON file to a Pydantic object.

    Returns:
        Union[CatalogV1, DbtBouncerManifest, RunResultsV4, RunResultsV5, RunResultsV6]:
            The dbt artifact loaded as a Pydantic object.

    Raises:
        FileNotFoundError:
            If the artifact file does not exist.

    """
    logging.debug(f"{artifact_name=}")
    logging.debug(f"{dbt_artifacts_dir=}")

    artifact_path = dbt_artifacts_dir / Path(artifact_name)
    logging.debug(f"Loading {artifact_name} from {artifact_path}...")
    if not Path(artifact_path).exists():
        raise FileNotFoundError(
            f"No {artifact_name} found at {artifact_path}.",
        )

    if artifact_name == "catalog.json":
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parser import (
                parse_catalog,
            )
        with Path.open(Path(artifact_path), "r") as fp:
            catalog_obj = parse_catalog(catalog=json.load(fp))

        return catalog_obj

    elif artifact_name == "manifest.json":
        # First assess dbt version is sufficient
        with Path.open(Path(artifact_path), "r") as fp:
            manifest_json = json.load(fp)

        assert (
            semver.Version.parse(manifest_json["metadata"]["dbt_version"]) >= "1.6.0"
        ), f"The supplied `manifest.json` was generated with dbt version {manifest_json['metadata']['dbt_version']}, this is below the minimum supported version of 1.6.0."

        manifest_obj = parse_manifest(manifest_json)

        return DbtBouncerManifest(**{"manifest": manifest_obj})

    elif artifact_name == "run_results.json":
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parser import (
                parse_run_results,
            )
        with Path.open(Path(artifact_path), "r") as fp:
            run_results_obj = parse_run_results(run_results=json.load(fp))

        return run_results_obj


def parse_catalog_artifact(
    artifact_dir: Path,
    manifest_obj: DbtBouncerManifest,
) -> tuple[List[DbtBouncerCatalogNode], List[DbtBouncerCatalogNode]]:
    """Parse the catalog.json artifact.

    Returns:
        List[DbtBouncerCatalogNode]: List of catalog nodes for the project.
        List[DbtBouncerCatalogNode]: List of catalog nodes for the project sources.

    """
    catalog_obj: CatalogV1 = load_dbt_artifact(
        artifact_name="catalog.json",
        dbt_artifacts_dir=artifact_dir,
    )
    project_catalog_nodes = [
        DbtBouncerCatalogNode(
            **{
                "catalog_node": v,
                "original_file_path": clean_path_str(
                    manifest_obj.manifest.nodes[k].original_file_path
                ),
                "unique_id": k,
            },
        )
        for k, v in catalog_obj.nodes.items()
        if k.split(".")[-2] == manifest_obj.manifest.metadata.project_name
    ]
    project_catalog_sources = [
        DbtBouncerCatalogNode(
            **{
                "catalog_node": v,
                "original_file_path": clean_path_str(
                    manifest_obj.manifest.sources[k].original_file_path
                ),
                "unique_id": k,
            },
        )
        for k, v in catalog_obj.sources.items()
        if k.split(".")[1] == manifest_obj.manifest.metadata.project_name
    ]
    logging.info(
        f"Parsed `catalog.json`: {len(project_catalog_nodes)} nodes, {len(project_catalog_sources)} sources.",
    )

    return project_catalog_nodes, project_catalog_sources


def parse_dbt_artifacts(
    bouncer_config: DbtBouncerConf, dbt_artifacts_dir: Path
) -> tuple[
    DbtBouncerManifest,
    List[Exposures],
    List[Macros],
    List[DbtBouncerModel],
    List[DbtBouncerSemanticModel],
    List[DbtBouncerSource],
    List[DbtBouncerTest],
    List[UnitTests],
    List[DbtBouncerCatalogNode],
    List[DbtBouncerCatalogNode],
    List[DbtBouncerRunResult],
]:
    """Parse all required dbt artifacts.

    Args:
        bouncer_config (DbtBouncerConf): All checks to be run.
        dbt_artifacts_dir (Path): Path to directory where artifacts are located.

    Returns:
        DbtBouncerManifest: The manifest object.
        List[DbtBouncerExposure]: List of exposures in the project.
        List[DbtBouncerMacro]: List of macros in the project.
        List[DbtBouncerModel]: List of models in the project.
        List[DbtBouncerSemanticModel]: List of semantic models in the project.
        List[DbtBouncerSource]: List of sources in the project.
        List[DbtBouncerTest]: List of tests in the project.
        List[DbtBouncerUnitTest]: List of unit tests in the project.
        List[DbtBouncerCatalogNode]: List of catalog nodes for the project.
        List[DbtBouncerCatalogNode]: List of catalog nodes for the project sources.
        List[DbtBouncerRunResult]: A list of DbtBouncerRunResult objects.

    """
    from dbt_bouncer.parsers import load_dbt_artifact, parse_manifest_artifact

    # Manifest, will always be parsed
    manifest_obj = load_dbt_artifact(
        artifact_name="manifest.json",
        dbt_artifacts_dir=dbt_artifacts_dir,
    )
    (
        project_exposures,
        project_macros,
        project_models,
        project_semantic_models,
        project_sources,
        project_tests,
        project_unit_tests,
    ) = parse_manifest_artifact(
        manifest_obj=manifest_obj,
    )

    # Catalog, must come after manifest is parsed
    from dbt_bouncer.parsers import parse_catalog_artifact

    if bouncer_config.catalog_checks != []:
        project_catalog_nodes, project_catalog_sources = parse_catalog_artifact(
            artifact_dir=dbt_artifacts_dir,
            manifest_obj=manifest_obj,
        )
    else:
        project_catalog_nodes = []
        project_catalog_sources = []

    # Run results, must come after manifest is parsed
    from dbt_bouncer.parsers import parse_run_results_artifact

    if bouncer_config.run_results_checks != []:
        project_run_results = parse_run_results_artifact(
            artifact_dir=dbt_artifacts_dir,
            manifest_obj=manifest_obj,
        )
    else:
        project_run_results = []

    return (
        manifest_obj,
        project_exposures,
        project_macros,
        project_models,
        project_semantic_models,
        project_sources,
        project_tests,
        project_unit_tests,
        project_catalog_nodes,
        project_catalog_sources,
        project_run_results,
    )


class DbtBouncerManifestV12(BaseParserModel):  # noqa: D101
    model_config = ConfigDict(
        extra="allow",
    )
    metadata: Metadata = Field(
        ..., description="Metadata about the manifest", title="ManifestMetadata"
    )
    nodes: Dict[
        str,
        Annotated[
            Union[
                DbtBouncerNodesSeed,
                DbtBouncerNodesTest,
                DbtBouncerNodesModel,
            ],
            Field(discriminator="resource_type"),
        ],
    ] = Field(
        ..., description="The nodes defined in the dbt project and its dependencies"
    )
    sources: Dict[str, Sources] = Field(
        ..., description="The sources defined in the dbt project and its dependencies"
    )
    macros: Dict[str, Macros] = Field(
        ..., description="The macros defined in the dbt project and its dependencies"
    )
    docs: Dict[str, Docs18] = Field(
        ..., description="The docs defined in the dbt project and its dependencies"
    )
    exposures: Dict[str, Exposures] = Field(
        ..., description="The exposures defined in the dbt project and its dependencies"
    )
    metrics: Dict[str, Metrics] = Field(
        ..., description="The metrics defined in the dbt project and its dependencies"
    )
    groups: Dict[str, Groups] = Field(
        ..., description="The groups defined in the dbt project"
    )
    selectors: Dict[str, Any] = Field(
        ..., description="The selectors defined in selectors.yml"
    )
    disabled: Optional[
        Dict[
            str,
            List[
                Union[
                    Disabled,
                    Disabled1,
                    Disabled2,
                    Disabled3,
                    Disabled4,
                    Disabled5,
                    Disabled6,
                    Disabled7,
                    Disabled8,
                    Disabled9,
                    Disabled10,
                    Disabled11,
                    Disabled12,
                    Disabled13,
                ]
            ],
        ]
    ] = Field(..., description="A mapping of the disabled nodes in the target")
    parent_map: Optional[Dict[str, List[str]]] = Field(
        ..., description="A mapping from\xa0child nodes to their dependencies"
    )
    child_map: Optional[Dict[str, List[str]]] = Field(
        ..., description="A mapping from parent nodes to their dependents"
    )
    group_map: Optional[Dict[str, List[str]]] = Field(
        ..., description="A mapping from group names to their nodes"
    )
    saved_queries: Dict[str, SavedQueries] = Field(
        ..., description="The saved queries defined in the dbt project"
    )
    semantic_models: Dict[str, SemanticModels] = Field(
        ..., description="The semantic models defined in the dbt project"
    )
    unit_tests: Dict[str, UnitTests] = Field(
        ..., description="The unit tests defined in the project"
    )


def parse_manifest(
    manifest: Dict[str, Any],
) -> Union[
    ManifestV10,
    ManifestV11,
    DbtBouncerManifestV12,
]:
    """Parse manifest.json.

    Args:
        manifest: A dict of manifest.json

    Raises:
        ValueError: If the manifest.json is not a valid manifest.json

    Returns:
       Union[ManifestV10, ManifestV11, DbtBouncerManifestV12]

    """
    dbt_schema_version = get_dbt_schema_version(artifact_json=manifest)
    if dbt_schema_version == ArtifactTypes.MANIFEST_V10.value.dbt_schema_version:
        return ManifestV10(**manifest)
    elif dbt_schema_version == ArtifactTypes.MANIFEST_V11.value.dbt_schema_version:
        return ManifestV11(**manifest)
    elif dbt_schema_version == ArtifactTypes.MANIFEST_V12.value.dbt_schema_version:
        return DbtBouncerManifestV12(**manifest)
    raise ValueError("Not a manifest.json")


def parse_manifest_artifact(
    manifest_obj: DbtBouncerManifest,
) -> tuple[
    List[Exposures],
    List[Macros],
    List[DbtBouncerModel],
    List[DbtBouncerSemanticModel],
    List[DbtBouncerSource],
    List[DbtBouncerTest],
    List[UnitTests],
]:
    """Parse the manifest.json artifact.

    Returns:
        List[DbtBouncerExposure]: List of exposures in the project.
        List[DbtBouncerMacro]: List of macros in the project.
        List[DbtBouncerModel]: List of models in the project.
        List[DbtBouncerSemanticModel]: List of semantic models in the project.
        List[DbtBouncerSource]: List of sources in the project.
        List[DbtBouncerTest]: List of tests in the project.
        List[DbtBouncerUnitTest]: List of unit tests in the project.

    """
    project_exposures = [
        v
        for _, v in manifest_obj.manifest.exposures.items()
        if v.package_name == manifest_obj.manifest.metadata.project_name
    ]
    project_macros = [
        v
        for _, v in manifest_obj.manifest.macros.items()
        if v.package_name == manifest_obj.manifest.metadata.project_name
    ]
    project_models = []
    project_tests = []
    for k, v in manifest_obj.manifest.nodes.items():
        if (
            isinstance(v.resource_type, Enum) and v.resource_type.value == "model"
        ) or v.resource_type == "model":
            if v.package_name == manifest_obj.manifest.metadata.project_name:
                # dbt-artifacts-parser does not support "versionless" dbt Cloud artifacts
                # https://github.com/yu-iskw/dbt-artifacts-parser/pull/112#issuecomment-2360298424
                # To work around this we create a more flexible Pydantic object and map to that
                if type(v) in [Nodes, Nodes2, Nodes4]:
                    v = DbtBouncerNodesModel(**v.model_dump())

                project_models.append(
                    DbtBouncerModel(
                        **{
                            "model": v,
                            "original_file_path": clean_path_str(v.original_file_path),
                            "unique_id": k,
                        },
                    ),
                )
        elif (
            (isinstance(v.resource_type, Enum) and v.resource_type.value == "test")
            or v.resource_type == "test"
        ) and v.package_name == manifest_obj.manifest.metadata.project_name:
            project_tests.append(
                DbtBouncerTest(
                    **{
                        "original_file_path": clean_path_str(v.original_file_path),
                        "test": v,
                        "unique_id": k,
                    },
                ),
            )

    if semver.Version.parse(manifest_obj.manifest.metadata.dbt_version) >= "1.8.0":
        project_unit_tests = [
            v
            for _, v in manifest_obj.manifest.unit_tests.items()
            if v.package_name == manifest_obj.manifest.metadata.project_name
        ]
    else:
        project_unit_tests = []

    project_semantic_models = [
        DbtBouncerSemanticModel(
            **{
                "original_file_path": clean_path_str(v.original_file_path),
                "semantic_model": v,
                "unique_id": k,
            },
        )
        for _, v in manifest_obj.manifest.semantic_models.items()
        if v.package_name == manifest_obj.manifest.metadata.project_name
    ]

    project_sources = [
        DbtBouncerSource(
            **{
                "original_file_path": clean_path_str(v.original_file_path),
                "source": v,
                "unique_id": k,
            },
        )
        for _, v in manifest_obj.manifest.sources.items()
        if v.package_name == manifest_obj.manifest.metadata.project_name
    ]

    logging.info(
        f"Parsed `manifest.json`, found `{manifest_obj.manifest.metadata.project_name}` project: {len(project_exposures)} exposures, {len(project_macros)} macros, {len(project_models)} nodes, {len(project_semantic_models)} semantic models, {len(project_sources)} sources, {len(project_tests)} tests, {len(project_unit_tests)} unit tests.",
    )
    return (
        project_exposures,
        project_macros,
        project_models,
        project_semantic_models,
        project_sources,
        project_tests,
        project_unit_tests,
    )


def parse_run_results_artifact(
    artifact_dir: Path,
    manifest_obj: DbtBouncerManifest,
) -> List[DbtBouncerRunResult]:
    """Parse the run_results.json artifact.

    Returns:
        List[DbtBouncerRunResult]: A list of DbtBouncerRunResult objects.

    """
    run_results_obj: Union[RunResultsV4, RunResultsV5, RunResultsV6] = (
        load_dbt_artifact(
            artifact_name="run_results.json",
            dbt_artifacts_dir=artifact_dir,
        )
    )

    project_run_results = [
        DbtBouncerRunResult(
            **{
                "original_file_path": (
                    clean_path_str(
                        manifest_obj.manifest.nodes[r.unique_id].original_file_path
                    )
                    if r.unique_id in manifest_obj.manifest.nodes
                    else clean_path_str(
                        manifest_obj.manifest.unit_tests[r.unique_id].original_file_path
                    )
                ),
                "run_result": r,
                "unique_id": r.unique_id,
            },
        )
        for r in run_results_obj.results
        if r.unique_id.split(".")[1] == manifest_obj.manifest.metadata.project_name
    ]
    logging.info(
        f"Parsed `run_results.json`: {len(project_run_results)} results.",
    )
    return project_run_results
