# mypy: disable-error-code="union-attr"


from typing import TYPE_CHECKING, List, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_common import (
        DbtBouncerExposureBase,
        DbtBouncerModelBase,
    )


class CheckExposureOnNonPublicModels(BaseCheck):
    """Exposures should be based on public models only.

    Receives:
        exposure (DbtBouncerExposureBase): The DbtBouncerExposureBase object to check.
        models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

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

    exposure: "DbtBouncerExposureBase" = Field(default=None)
    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_exposure_based_on_non_public_models"]

    def execute(self) -> None:
        """Execute the check."""
        non_public_upstream_dependencies = []
        for model in self.exposure.depends_on.nodes:
            if (
                model.split(".")[0] == "model"
                and model.split(".")[1] == self.exposure.package_name
            ):
                model = next(m for m in self.models if m.unique_id == model)
                if model.access.value != "public":
                    non_public_upstream_dependencies.append(model.name)

        assert not non_public_upstream_dependencies, (
            f"`{self.exposure.name}` is based on a model(s) that is not public: {non_public_upstream_dependencies}."
        )


class CheckExposureOnView(BaseCheck):
    """Exposures should not be based on views.

    Parameters:
        materializations_to_include (Optional[List[str]]): List of materializations to include in the check.

    Receives:
        exposure (DbtBouncerExposureBase): The DbtBouncerExposureBase object to check.
        models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

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

    exposure: "DbtBouncerExposureBase" = Field(default=None)
    materializations_to_include: List[str] = Field(
        default=["ephemeral", "view"],
    )
    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_exposure_based_on_view"]

    def execute(self) -> None:
        """Execute the check."""
        non_table_upstream_dependencies = []
        for model in self.exposure.depends_on.nodes:
            if (
                model.split(".")[0] == "model"
                and model.split(".")[1] == self.exposure.package_name
            ):
                model = next(m for m in self.models if m.unique_id == model)
                if model.config.materialized in self.materializations_to_include:
                    non_table_upstream_dependencies.append(model.name)

        assert not non_table_upstream_dependencies, (
            f"`{self.exposure.name}` is based on a model that is not a table: {non_table_upstream_dependencies}."
        )
