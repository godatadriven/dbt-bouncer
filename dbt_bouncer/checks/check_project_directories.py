from typing import Literal

import pytest

from dbt_bouncer.config_validator_base import BaseCheck
from dbt_bouncer.utils import get_check_inputs


class CheckTopLevelDirectories(BaseCheck):
    name: Literal["check_top_level_directories"]


@pytest.mark.iterate_over_models
def check_top_level_directories(request, model=None):
    """
    Only specified top-level directories are allowed to contain models.
    """

    model = get_check_inputs(model=model, request=request)["model"]
    top_level_dir = model["path"].split("/")[0]
    assert top_level_dir in [
        "staging",
        "intermediate",
        "marts",
    ], f"{model['unique_id']} is located in `{top_level_dir}`, this is not a valid top-level directory."
