import logging
import warnings
from typing import TYPE_CHECKING

from pydantic import BaseModel

from dbt_bouncer.artifact_parsers.dbt_cloud.catalog_latest import CatalogLatest
from dbt_bouncer.artifact_parsers.dbt_cloud.catalog_latest import (
    Nodes as CatalogNodesLatest,
)
from dbt_bouncer.artifact_parsers.dbt_cloud.catalog_latest import (
    Sources as CatalogSourcesLatest,
)
from dbt_bouncer.utils import clean_path_str

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
        CatalogV1,
    )
    from dbt_artifacts_parser.parsers.catalog.catalog_v1 import Nodes as CatalogNodes
    from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
        Sources as CatalogSources,
    )
if TYPE_CHECKING:
    from pathlib import Path

    from dbt_bouncer.artifact_parsers.parsers_manifest import DbtBouncerManifest


from dbt_bouncer.artifact_parsers.parsers_common import load_dbt_artifact


class DbtBouncerCatalog(BaseModel):
    """Model for all catalog objects."""

    catalog: CatalogV1 | CatalogLatest


class DbtBouncerCatalogNode(BaseModel):
    """Model for all nodes in `catalog.json`."""

    catalog_node: (
        CatalogNodes | CatalogNodesLatest | CatalogSources | CatalogSourcesLatest
    )
    original_file_path: str
    unique_id: str


def parse_catalog(
    artifact_dir: "Path",
    manifest_obj: "DbtBouncerManifest",
    package_name: str | None = None,
) -> tuple[list[DbtBouncerCatalogNode], list[DbtBouncerCatalogNode]]:
    """Parse the catalog.json artifact.

    Returns:
        list[DbtBouncerCatalogNode]: List of catalog nodes for the project.
        list[DbtBouncerCatalogNode]: List of catalog nodes for the project sources.

    Raises:
        TypeError: If the loaded artifact is not of the expected type.

    """
    catalog_obj = load_dbt_artifact(
        artifact_name="catalog.json",
        dbt_artifacts_dir=artifact_dir,
    )
    if not isinstance(catalog_obj, DbtBouncerCatalog):
        raise TypeError(f"Expected DbtBouncerCatalog, got {type(catalog_obj)}")

    project_catalog_nodes = [
        DbtBouncerCatalogNode(
            catalog_node=v,
            original_file_path=str(
                clean_path_str(manifest_obj.manifest.nodes[k].original_file_path)
            ),
            unique_id=k,
        )
        for k, v in catalog_obj.catalog.nodes.items()
        if k.split(".")[-2]
        == (package_name or manifest_obj.manifest.metadata.project_name)
    ]
    project_catalog_sources = [
        DbtBouncerCatalogNode(
            catalog_node=v,
            original_file_path=str(
                clean_path_str(manifest_obj.manifest.sources[k].original_file_path)
            ),
            unique_id=k,
        )
        for k, v in catalog_obj.catalog.sources.items()
        if k.split(".")[1]
        == (package_name or manifest_obj.manifest.metadata.project_name)
    ]
    logging.info(
        f"Parsed `catalog.json`: {len(project_catalog_nodes)} nodes, {len(project_catalog_sources)} sources.",
    )

    return project_catalog_nodes, project_catalog_sources
