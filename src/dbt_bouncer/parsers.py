from typing import Union

from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogTable
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import ManifestV10
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import (
    ModelNode as ModelNode_v10,
)
from dbt_artifacts_parser.parsers.manifest.manifest_v10 import (
    SourceDefinition as SourceDefinition_v10,
)
from dbt_artifacts_parser.parsers.manifest.manifest_v11 import ManifestV11
from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
    ModelNode as ModelNode_v11,
)
from dbt_artifacts_parser.parsers.manifest.manifest_v11 import (
    SourceDefinition as SourceDefinition_v11,
)
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (
    ManifestV12,
    Nodes4,
    Sources,
)
from dbt_artifacts_parser.parsers.run_results.run_results_v4 import (
    RunResultOutput as RunResultOutput_v4,
)
from dbt_artifacts_parser.parsers.run_results.run_results_v5 import (
    RunResultOutput as RunResultOutput_v5,
)
from dbt_artifacts_parser.parsers.run_results.run_results_v6 import Result
from pydantic import BaseModel


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
