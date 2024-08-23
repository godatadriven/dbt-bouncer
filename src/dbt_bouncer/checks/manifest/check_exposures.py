# mypy: disable-error-code="union-attr"

import warnings
from typing import List, Literal, Union

import pytest
from _pytest.fixtures import TopRequest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Exposures

from pydantic import Field

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.parsers import DbtBouncerModel
from dbt_bouncer.utils import bouncer_check


class CheckExposureOnNonPublicModels(BaseCheck):
    name: Literal["check_exposure_based_on_non_public_models"]


@pytest.mark.iterate_over_exposures
@bouncer_check
def check_exposure_based_on_non_public_models(
    models: List[DbtBouncerModel],
    request: TopRequest,
    exposure: Union[Exposures, None] = None,
    **kwargs,
) -> None:
    """
    Exposures should be based on public models only.

    Receives:
        exclude (Optional[str]): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match the pattern will not be checked.
        exposure (Exposures): The Exposures object to check.
        include (Optional[str]): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match the pattern will be checked.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_exposure_based_on_non_public_models
        ```
    """

    non_public_upstream_dependencies = []
    for model in exposure.depends_on.nodes:
        if model.split(".")[0] == "model" and model.split(".")[1] == exposure.package_name:
            model = [m for m in models if m.unique_id == model][0]
            if model.access.value != "public":
                non_public_upstream_dependencies.append(model.name)

    assert (
        not non_public_upstream_dependencies
    ), f"`{exposure.name}` is based on a model(s) that is not public: {non_public_upstream_dependencies}."


class CheckExposureOnView(BaseCheck):
    materializations_to_include: List[str] = Field(
        default=["ephemeral", "view"],
    )
    name: Literal["check_exposure_based_on_view"]


@pytest.mark.iterate_over_exposures
@bouncer_check
def check_exposure_based_on_view(
    models: List[DbtBouncerModel],
    request: TopRequest,
    exposure: Union[Exposures, None] = None,
    materializations_to_include: Union[List[str], None] = None,
    **kwargs,
) -> None:
    """
    Exposures should not be based on views.

    Receives:
        exclude (Optional[str]): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match the pattern will not be checked.
        exposure (Exposures): The Exposures object to check.
        include (Optional[str]): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match the pattern will be checked.
        materializations_to_include (Optional[List[str]]): List of materializations to include in the check. If not provided, defaults to `ephemeral` and `view`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_exposure_based_on_view
        ```
        ```yaml
        manifest_checks:
            - name: check_exposure_based_on_view
              materializations_to_include:
                - ephemeral
                - my_custom_materialization
                - view
        ```
    """

    non_table_upstream_dependencies = []
    for model in exposure.depends_on.nodes:
        if model.split(".")[0] == "model" and model.split(".")[1] == exposure.package_name:
            model = [m for m in models if m.unique_id == model][0]
            if model.config.materialized in materializations_to_include:  # type: ignore[operator]
                non_table_upstream_dependencies.append(model.name)

    assert (
        not non_table_upstream_dependencies
    ), f"`{exposure.name}` is based on a model that is not a table: {non_table_upstream_dependencies}."
