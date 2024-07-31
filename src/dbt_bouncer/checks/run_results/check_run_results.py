from typing import Literal

import pytest
from pydantic import Field

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.utils import get_check_inputs


class CheckRunResultsMaxExecutionTime(BaseCheck):
    max_execution_time: float = Field(
        description="The maximum execution time (seconds) allowed for a node."
    )
    name: Literal["check_run_results_max_execution_time"]


@pytest.mark.iterate_over_run_results
def check_run_results_max_execution_time(request, check_config=None, run_result=None) -> None:
    """
    Each result can take a maximum duration (seconds).
    """

    input_vars = get_check_inputs(
        check_config=check_config, run_result=run_result, request=request
    )
    check_config = input_vars["check_config"]
    run_result = input_vars["run_result"]

    from dbt_bouncer.logger import logger

    logger.warning(f"{check_config=}")
    logger.warning(f"{run_result=}")

    assert (
        run_result["execution_time"] <= check_config["max_execution_time"]
    ), f"`{run_result['unique_id'].split('.')[-1]}` has an execution time ({run_result['execution_time']} greater than permitted ({check_config['max_execution_time']}s)."
