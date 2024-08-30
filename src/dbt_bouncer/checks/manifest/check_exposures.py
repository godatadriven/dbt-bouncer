# mypy: disable-error-code="union-attr"


from typing import TYPE_CHECKING, List, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    import warnings

    from dbt_bouncer.parsers import DbtBouncerModel

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Exposures


class CheckExposureOnNonPublicModels(BaseCheck):
    name: Literal["check_exposure_based_on_non_public_models"]


def check_exposure_based_on_non_public_models(
    exposure: "Exposures",
    models: List["DbtBouncerModel"],
    **kwargs,
) -> None:
    """Exposures should be based on public models only.

    Parameters:
        exposure (Exposures): The Exposures object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_exposure_based_on_non_public_models
        ```

    """
    non_public_upstream_dependencies = []
    for model in exposure.depends_on.nodes:
        if (
            model.split(".")[0] == "model"
            and model.split(".")[1] == exposure.package_name
        ):
            model = next(m for m in models if m.unique_id == model)
            if model.access.value != "public":
                non_public_upstream_dependencies.append(model.name)

    assert not non_public_upstream_dependencies, f"`{exposure.name}` is based on a model(s) that is not public: {non_public_upstream_dependencies}."


class CheckExposureOnView(BaseCheck):
    materializations_to_include: List[str] = Field(
        default=["ephemeral", "view"],
    )
    name: Literal["check_exposure_based_on_view"]


def check_exposure_based_on_view(
    exposure: "Exposures",
    models: List["DbtBouncerModel"],
    materializations_to_include: List[str] = ["ephemeral", "view"],  # noqa: B006
    **kwargs,
) -> None:
    """Exposures should not be based on views.

    Parameters:
        exposure (Exposures): The Exposures object to check.
        materializations_to_include (Optional[List[str]]): List of materializations to include in the check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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
        if (
            model.split(".")[0] == "model"
            and model.split(".")[1] == exposure.package_name
        ):
            model = next(m for m in models if m.unique_id == model)
            if model.config.materialized in materializations_to_include:
                non_table_upstream_dependencies.append(model.name)

    assert not non_table_upstream_dependencies, f"`{exposure.name}` is based on a model that is not a table: {non_table_upstream_dependencies}."
