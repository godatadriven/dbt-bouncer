from typing import List, Literal

import pytest
from pydantic import Field

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.utils import get_check_inputs


class CheckExposureOnNonPublicModels(BaseCheck):
    name: Literal["check_exposure_based_on_non_public_models"]


@pytest.mark.iterate_over_exposures
def check_exposure_based_on_non_public_models(models, request, exposure=None):
    """
    Exposures should be based on public models only.
    """

    exposure = get_check_inputs(exposure=exposure, request=request)["exposure"]

    non_public_upstream_dependencies = []
    for model in exposure["depends_on"]["nodes"]:
        if (
            model.split(".")[0] == "model"
            and model.split(".")[1] == exposure["unique_id"].split(".")[1]
        ):
            model = [m for m in models if m["unique_id"] == model][0]
            if model["access"] != "public":
                non_public_upstream_dependencies.append(model["unique_id"].split(".")[-1])

    assert (
        not non_public_upstream_dependencies
    ), f"`{exposure['unique_id'].split('.')[-1]}` is based on a model(s) that is not public: {non_public_upstream_dependencies}."


class CheckExposureOnView(BaseCheck):
    materializations_to_include: List[str] = Field(
        default=["ephemeral", "view"],
        description="List of materializations to include in the check. If not provided, defaults to `ephemeral` and `view`.",
    )
    name: Literal["check_exposure_based_on_view"]


@pytest.mark.iterate_over_exposures
def check_exposure_based_on_view(models, request, check_config=None, exposure=None):
    """
    Exposures should not be based on views.
    """

    input_vars = get_check_inputs(check_config=check_config, exposure=exposure, request=request)
    exposure = input_vars["exposure"]
    materializations_to_include = input_vars["check_config"]["materializations_to_include"]

    non_table_upstream_dependencies = []
    for model in exposure["depends_on"]["nodes"]:
        if (
            model.split(".")[0] == "model"
            and model.split(".")[1] == exposure["unique_id"].split(".")[1]
        ):
            model = [m for m in models if m["unique_id"] == model][0]
            if model["config"]["materialized"] in materializations_to_include:
                non_table_upstream_dependencies.append(model["unique_id"].split(".")[-1])

    assert (
        not non_table_upstream_dependencies
    ), f"`{exposure['unique_id'].split('.')[-1]}` is based on a model that is not a table: {non_table_upstream_dependencies}."
