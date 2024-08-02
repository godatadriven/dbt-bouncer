import re
from typing import Literal

import pytest
from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.utils import get_check_inputs


class CheckLineagePermittedUpstreamModels(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include: str = Field(
        description="Regex pattern to match the model path. Only model paths that match the pattern will be checked."
    )
    name: Literal["check_lineage_permitted_upstream_models"]
    upstream_path_pattern: str = Field(
        description="Regexp pattern to match the upstream model(s) path."
    )


@pytest.mark.iterate_over_models
def check_lineage_permitted_upstream_models(
    manifest_obj, models, request, check_config=None, model=None
):
    """
    Upstream models must have a path that matches the provided `upstream_path_pattern`.
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    check_config = input_vars["check_config"]
    model = input_vars["model"]

    upstream_models = [
        x
        for x in model["depends_on"].get("nodes")
        if x.split(".")[0] == "model" and x.split(".")[1] == manifest_obj.metadata.project_name
    ]
    not_permitted_upstream_models = [
        upstream_model
        for upstream_model in upstream_models
        if re.compile(check_config["upstream_path_pattern"].strip()).match(
            [m for m in models if m["unique_id"] == upstream_model][0].get("path")
        )
        is None
    ]
    assert (
        not not_permitted_upstream_models
    ), f"`{model['unique_id'].split('.')[-1]}` references upstream models that are not permitted: {[m.split('.')[-1] for m in not_permitted_upstream_models]}."


class CheckLineageSeedCannotBeUsed(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include: str = Field(
        description="Regex pattern to match the model path. Only model paths that match the pattern will be checked."
    )
    name: Literal["check_lineage_seed_cannot_be_used"]


@pytest.mark.iterate_over_models
def check_lineage_seed_cannot_be_used(request, model=None):
    """
    Seed cannot be referenced in models with a path that matches the specified `include` config.
    """

    model = get_check_inputs(model=model, request=request)["model"]

    assert not [
        x for x in model["depends_on"].get("nodes") if x.split(".")[0] == "seed"
    ], f"`{model['unique_id'].split('.')[-1]}` references a seed even though this is not permitted."


class CheckLineageSourceCannotBeUsed(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include: str = Field(
        description="Regex pattern to match the model path. Only model paths that match the pattern will be checked."
    )
    name: Literal["check_lineage_source_cannot_be_used"]


@pytest.mark.iterate_over_models
def check_lineage_source_cannot_be_used(request, model=None):
    """
    Sources cannot be referenced in models with a path that matches the specified `include` config.
    """

    model = get_check_inputs(model=model, request=request)["model"]

    assert not [
        x for x in model["depends_on"].get("nodes") if x.split(".")[0] == "source"
    ], f"`{model['unique_id'].split('.')[-1]}` references a source even though this is not permitted."
