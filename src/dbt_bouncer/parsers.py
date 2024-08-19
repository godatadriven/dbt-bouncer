# TODO Remove after this program no longer support Python 3.8.*
from __future__ import annotations

import json
import warnings
from enum import Enum
from pathlib import Path
from typing import List, Literal, Union

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parser import parse_catalog, parse_manifest, parse_run_results
    from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogTable, CatalogV1
    from dbt_artifacts_parser.parsers.manifest.manifest_v10 import (
        GenericTestNode as GenericTestNode_v10,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v10 import ManifestV10
    from dbt_artifacts_parser.parsers.manifest.manifest_v10 import (
        ModelNode as ModelNode_v10,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v10 import (
        SourceDefinition as SourceDefinition_v10,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
        GenericTestNode as GenericTestNode_v11,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import ManifestV11
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
        ModelNode as ModelNode_v11,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
        SourceDefinition as SourceDefinition_v11,
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (
        Exposures,
        Macros,
        ManifestV12,
        Nodes4,
        Nodes6,
        Sources,
    )
    from dbt_artifacts_parser.parsers.run_results.run_results_v4 import (
        RunResultOutput as RunResultOutput_v4,
    )
    from dbt_artifacts_parser.parsers.run_results.run_results_v4 import RunResultsV4
    from dbt_artifacts_parser.parsers.run_results.run_results_v5 import (
        RunResultOutput as RunResultOutput_v5,
    )
    from dbt_artifacts_parser.parsers.run_results.run_results_v5 import RunResultsV5
    from dbt_artifacts_parser.parsers.run_results.run_results_v6 import Result, RunResultsV6
    from pydantic import BaseModel

from dbt_bouncer.logger import logger


class DbtBouncerCatalogNode(BaseModel):
    node: CatalogTable
    path: str
    unique_id: str


class DbtBouncerManifest(BaseModel):
    manifest: Union[ManifestV10, ManifestV11, ManifestV12]


class DbtBouncerModel(BaseModel):
    model: Union[ModelNode_v10, ModelNode_v11, Nodes4]
    path: str
    unique_id: str


class DbtBouncerResult(BaseModel):
    path: str
    result: Union[RunResultOutput_v4, RunResultOutput_v5, Result]
    unique_id: str


class DbtBouncerSource(BaseModel):
    path: str
    source: Union[SourceDefinition_v10, SourceDefinition_v11, Sources]
    unique_id: str


class DbtBouncerTest(BaseModel):
    path: str
    test: Union[GenericTestNode_v10, GenericTestNode_v11, Nodes6]
    unique_id: str


def load_dbt_artifact(
    artifact_name: Literal["catalog.json", "manifest.json", "run_results.json"],
    dbt_artifacts_dir: Path,
) -> Union[CatalogV1, DbtBouncerManifest, RunResultsV4, RunResultsV5, RunResultsV6]:
    """
    Load a dbt artifact from a JSON file to a Pydantic object
    """

    logger.debug(f"{artifact_name=}")
    logger.debug(f"{dbt_artifacts_dir=}")

    artifact_path = dbt_artifacts_dir / Path(artifact_name)
    logger.info(f"Loading {artifact_name} from {artifact_path.absolute()}...")
    if not artifact_path.exists():
        raise FileNotFoundError(f"No {artifact_name} found at {artifact_path.absolute()}.")

    if artifact_name == "catalog.json":
        with Path.open(artifact_path, "r") as fp:
            catalog_obj = parse_catalog(catalog=json.load(fp))

        return catalog_obj

    elif artifact_name == "manifest.json":
        with Path.open(artifact_path, "r") as fp:
            manifest_obj = parse_manifest(manifest=json.load(fp))

        return DbtBouncerManifest(**{"manifest": manifest_obj})

    elif artifact_name == "run_results.json":
        with Path.open(artifact_path, "r") as fp:
            run_results_obj = parse_run_results(run_results=json.load(fp))

        return run_results_obj


def parse_catalog_artifact(
    artifact_dir: Path, manifest_obj: DbtBouncerManifest
) -> tuple[List[DbtBouncerCatalogNode], List[DbtBouncerCatalogNode]]:
    catalog_obj: CatalogV1 = load_dbt_artifact(
        artifact_name="catalog.json",
        dbt_artifacts_dir=artifact_dir,
    )

    project_catalog_nodes = [
        DbtBouncerCatalogNode(
            **{
                "node": v,
                "path": manifest_obj.manifest.nodes[k].path,
                "unique_id": k,
            }
        )
        for k, v in catalog_obj.nodes.items()
        if k.split(".")[-2] == manifest_obj.manifest.metadata.project_name
    ]
    project_catalog_sources = [
        DbtBouncerCatalogNode(
            **{
                "node": v,
                "path": manifest_obj.manifest.sources[k].path,
                "unique_id": k,
            }
        )
        for k, v in catalog_obj.sources.items()
        if k.split(".")[1] == manifest_obj.manifest.metadata.project_name
    ]
    logger.info(
        f"Parsed `catalog.json`, found {len(project_catalog_nodes)} nodes and {len(project_catalog_sources)} sources."
    )

    return project_catalog_nodes, project_catalog_sources


def parse_manifest_artifact(artifact_dir: Path, manifest_obj: DbtBouncerManifest) -> tuple[
    List[Exposures],
    List[Macros],
    List[DbtBouncerModel],
    List[DbtBouncerTest],
    List[DbtBouncerSource],
]:
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
            if (
                v.package_name == manifest_obj.manifest.metadata.project_name
            ):  # dbt 1.6  # dbt 1.7+
                project_models.append(
                    DbtBouncerModel(**{"model": v, "path": v.path, "unique_id": k})
                )
        elif (
            isinstance(v.resource_type, Enum) and v.resource_type.value == "test"
        ) or v.resource_type == "test":
            if v.package_name == manifest_obj.manifest.metadata.project_name:
                project_tests.append(DbtBouncerTest(**{"path": v.path, "test": v, "unique_id": k}))

    project_sources = [
        DbtBouncerSource(**{"source": v, "path": v.path, "unique_id": k})
        for _, v in manifest_obj.manifest.sources.items()
        if v.package_name == manifest_obj.manifest.metadata.project_name
    ]
    logger.info(
        f"Parsed `manifest.json`, found `{manifest_obj.manifest.metadata.project_name}` project, found {len(project_exposures)} exposures, {len(project_macros)} macros, {len(project_models)} nodes, {len(project_sources)} sources and {len(project_tests)} tests."
    )

    return project_exposures, project_macros, project_models, project_tests, project_sources


def parse_run_results_artifact(
    artifact_dir: Path, manifest_obj: DbtBouncerManifest
) -> List[DbtBouncerResult]:
    run_results_obj: Union[RunResultsV4, RunResultsV5, RunResultsV6] = load_dbt_artifact(
        artifact_name="run_results.json",
        dbt_artifacts_dir=artifact_dir,
    )
    project_run_results = [
        DbtBouncerResult(
            **{
                "path": manifest_obj.manifest.nodes[r.unique_id].path,
                "result": r,
                "unique_id": r.unique_id,
            }
        )
        for r in run_results_obj.results
        if r.unique_id.split(".")[1] == manifest_obj.manifest.metadata.project_name
    ]
    logger.info(f"Parsed `run_results.json`, found {len(project_run_results)} results.")
    return project_run_results
