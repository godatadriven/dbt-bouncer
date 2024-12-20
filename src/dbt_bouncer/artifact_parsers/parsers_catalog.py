import logging
import warnings
from typing import TYPE_CHECKING, List, Union

from pydantic import BaseModel

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


class DbtBouncerCatalogNode(BaseModel):
    """Model for all nodes in `catalog.json`."""

    catalog_node: Union[CatalogNodes, CatalogSources]
    original_file_path: str
    unique_id: str


def parse_catalog(
    artifact_dir: "Path",
    manifest_obj: "DbtBouncerManifest",
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
