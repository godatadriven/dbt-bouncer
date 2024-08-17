from typing import Union

from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogTable
from dbt_artifacts_parser.parsers.run_results.run_results_v4 import (
    RunResultOutput as RunResultOutputv4,
)
from dbt_artifacts_parser.parsers.run_results.run_results_v5 import (
    RunResultOutput as RunResultOutputv5,
)
from dbt_artifacts_parser.parsers.run_results.run_results_v6 import Result
from pydantic import BaseModel


class DbtBouncerCatalog(BaseModel):
    node: CatalogTable
    path: str
    unique_id: str


class DbtBouncerResult(BaseModel):
    path: str
    result: Union[RunResultOutputv4, RunResultOutputv5, Result]
