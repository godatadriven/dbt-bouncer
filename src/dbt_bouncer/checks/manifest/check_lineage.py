# mypy: disable-error-code="union-attr"

import re
from typing import List, Literal, Optional, Union

import pytest
from _pytest.fixtures import TopRequest
from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.parsers import DbtBouncerManifest, DbtBouncerModel
from dbt_bouncer.utils import bouncer_check


class CheckLineagePermittedUpstreamModels(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include: str = Field(
        description="Regex pattern to match the model path. Only model paths that match the pattern will be checked."
    )
    index: Optional[int] = Field(
        default=None, description="Index to uniquely identify the check, calculated at runtime."
    )
    name: Literal["check_lineage_permitted_upstream_models"]
    upstream_path_pattern: str = Field(
        description="Regexp pattern to match the upstream model(s) path."
    )


@pytest.mark.iterate_over_models
@bouncer_check
def check_lineage_permitted_upstream_models(
    manifest_obj: DbtBouncerManifest,
    models: List[DbtBouncerModel],
    request: TopRequest,
    model: Union[DbtBouncerModel, None] = None,
    upstream_path_pattern: Union[None, str] = None,
    **kwargs,
) -> None:
    """
    Upstream models must have a path that matches the provided `upstream_path_pattern`.
    """

    upstream_models = [
        x
        for x in model.depends_on.nodes
        if x.split(".")[0] == "model"
        and x.split(".")[1] == manifest_obj.manifest.metadata.project_name
    ]
    not_permitted_upstream_models = [
        upstream_model
        for upstream_model in upstream_models
        if re.compile(upstream_path_pattern.strip()).match(
            [m for m in models if m.unique_id == upstream_model][0].path
        )
        is None
    ]
    assert (
        not not_permitted_upstream_models
    ), f"`{model.unique_id.split('.')[-1]}` references upstream models that are not permitted: {[m.split('.')[-1] for m in not_permitted_upstream_models]}."


class CheckLineageSeedCannotBeUsed(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include: str = Field(
        description="Regex pattern to match the model path. Only model paths that match the pattern will be checked."
    )
    index: Optional[int] = Field(
        default=None, description="Index to uniquely identify the check, calculated at runtime."
    )
    name: Literal["check_lineage_seed_cannot_be_used"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_lineage_seed_cannot_be_used(
    request: TopRequest, model: Union[DbtBouncerModel, None] = None, **kwargs
) -> None:
    """
    Seed cannot be referenced in models with a path that matches the specified `include` config.
    """

    assert not [
        x for x in model.depends_on.nodes if x.split(".")[0] == "seed"
    ], f"`{model.unique_id.split('.')[-1]}` references a seed even though this is not permitted."


class CheckLineageSourceCannotBeUsed(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include: str = Field(
        description="Regex pattern to match the model path. Only model paths that match the pattern will be checked."
    )
    index: Optional[int] = Field(
        default=None, description="Index to uniquely identify the check, calculated at runtime."
    )
    name: Literal["check_lineage_source_cannot_be_used"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_lineage_source_cannot_be_used(
    request: TopRequest, model: Union[DbtBouncerModel, None] = None, **kwargs
) -> None:
    """
    Sources cannot be referenced in models with a path that matches the specified `include` config.
    """

    assert not [
        x for x in model.depends_on.nodes if x.split(".")[0] == "source"
    ], f"`{model.unique_id.split('.')[-1]}` references a source even though this is not permitted."
