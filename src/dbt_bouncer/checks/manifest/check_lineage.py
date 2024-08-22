# mypy: disable-error-code="union-attr"

import re
from typing import List, Literal, Union

import pytest
from _pytest.fixtures import TopRequest

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.parsers import DbtBouncerManifest, DbtBouncerModel
from dbt_bouncer.utils import bouncer_check


class CheckLineagePermittedUpstreamModels(BaseCheck):
    name: Literal["check_lineage_permitted_upstream_models"]
    upstream_path_pattern: str


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

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        upstream_path_pattern (str): Regexp pattern to match the upstream model(s) path.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_lineage_permitted_upstream_models
              include: ^staging
              upstream_path_pattern: $^
            - name: check_lineage_permitted_upstream_models
              include: ^intermediate
              upstream_path_pattern: ^staging|^intermediate
            - name: check_lineage_permitted_upstream_models
              include: ^marts
              upstream_path_pattern: ^staging|^intermediate
        ```
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
    ), f"`{model.name}` references upstream models that are not permitted: {[m.split('.')[-1] for m in not_permitted_upstream_models]}."


class CheckLineageSeedCannotBeUsed(BaseCheck):
    name: Literal["check_lineage_seed_cannot_be_used"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_lineage_seed_cannot_be_used(
    request: TopRequest, model: Union[DbtBouncerModel, None] = None, **kwargs
) -> None:
    """
    Seed cannot be referenced in models with a path that matches the specified `include` config.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_lineage_seed_cannot_be_used
              include: ^intermediate|^marts
        ```
    """

    assert not [
        x for x in model.depends_on.nodes if x.split(".")[0] == "seed"
    ], f"`{model.name}` references a seed even though this is not permitted."


class CheckLineageSourceCannotBeUsed(BaseCheck):
    name: Literal["check_lineage_source_cannot_be_used"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_lineage_source_cannot_be_used(
    request: TopRequest, model: Union[DbtBouncerModel, None] = None, **kwargs
) -> None:
    """
    Sources cannot be referenced in models with a path that matches the specified `include` config.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_lineage_source_cannot_be_used
              include: ^intermediate|^marts
        ```
    """

    assert not [
        x for x in model.depends_on.nodes if x.split(".")[0] == "source"
    ], f"`{model.name}` references a source even though this is not permitted."
