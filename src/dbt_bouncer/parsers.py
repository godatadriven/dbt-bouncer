from dbt_artifacts_parser.parsers.run_results.run_results_v6 import Result
from pydantic import BaseModel


class DbtBouncerResult(BaseModel):
    path: str
    result: Result
