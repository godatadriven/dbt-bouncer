from typing import TYPE_CHECKING, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerExposureBase,
        DbtBouncerModelBase,
    )

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class CheckExposureOnModel(BaseCheck):
    """Exposures should depend on a model.

    Parameters:
        maximum_number_of_models (int | None): The maximum number of models an exposure can depend on, defaults to 100.
        minimum_number_of_models (int | None): The minimum number of models an exposure can depend on, defaults to 1.

    Receives:
        exposure (DbtBouncerExposureBase): The DbtBouncerExposureBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_exposure_based_on_model
        ```
        ```yaml
        manifest_checks:
            - name: check_exposure_based_on_model
              maximum_number_of_models: 3
              minimum_number_of_models: 1
        ```

    """

    exposure: "DbtBouncerExposureBase | None" = Field(default=None)
    maximum_number_of_models: int = Field(default=100)
    minimum_number_of_models: int = Field(default=1)
    name: Literal["check_exposure_based_on_model"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If upstream models number is not within limits.

        """
        if self.exposure is None:
            raise DbtBouncerFailedCheckError("self.exposure is None")
        depends_on = self.exposure.depends_on
        number_of_upstream_models = (
            len(getattr(depends_on, "nodes", []) or []) if depends_on else 0
        )

        if number_of_upstream_models < self.minimum_number_of_models:
            raise DbtBouncerFailedCheckError(
                f"`{self.exposure.name}` is based on less models ({number_of_upstream_models}) than the minimum permitted ({self.minimum_number_of_models})."
            )
        if number_of_upstream_models > self.maximum_number_of_models:
            raise DbtBouncerFailedCheckError(
                f"`{self.exposure.name}` is based on more models ({number_of_upstream_models}) than the maximum permitted ({self.maximum_number_of_models})."
            )


class CheckExposureOnNonPublicModels(BaseCheck):
    """Exposures should be based on public models only.

    Receives:
        exposure (DbtBouncerExposureBase): The DbtBouncerExposureBase object to check.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_exposure_based_on_non_public_models
        ```

    """

    exposure: "DbtBouncerExposureBase | None" = Field(default=None)
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_exposure_based_on_non_public_models"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If exposure is based on non-public models.

        """
        if self.exposure is None:
            raise DbtBouncerFailedCheckError("self.exposure is None")
        non_public_upstream_dependencies = []
        for model in getattr(self.exposure.depends_on, "nodes", []) or []:
            if (
                next(m for m in self.models if m.unique_id == model).resource_type
                == "model"
                and next(m for m in self.models if m.unique_id == model).package_name
                == self.exposure.package_name
            ):
                model_obj = next(m for m in self.models if m.unique_id == model)
                if model_obj.access and model_obj.access.value != "public":
                    non_public_upstream_dependencies.append(model_obj.name)

        if non_public_upstream_dependencies:
            raise DbtBouncerFailedCheckError(
                f"`{self.exposure.name}` is based on a model(s) that is not public: {non_public_upstream_dependencies}."
            )


class CheckExposureOnView(BaseCheck):
    """Exposures should not be based on views.

    Parameters:
        materializations_to_include (list[str] | None): List of materializations to include in the check.

    Receives:
        exposure (DbtBouncerExposureBase): The DbtBouncerExposureBase object to check.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

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

    exposure: "DbtBouncerExposureBase | None" = Field(default=None)
    materializations_to_include: list[str] = Field(
        default=["ephemeral", "view"],
    )
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_exposure_based_on_view"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If exposure is based on a model that is not a table.

        """
        if self.exposure is None:
            raise DbtBouncerFailedCheckError("self.exposure is None")
        non_table_upstream_dependencies = []
        for model in getattr(self.exposure.depends_on, "nodes", []) or []:
            if (
                next(m for m in self.models if m.unique_id == model).resource_type
                == "model"
                and next(m for m in self.models if m.unique_id == model).package_name
                == self.exposure.package_name
            ):
                model_obj = next(m for m in self.models if m.unique_id == model)
                if model_obj.config.materialized in self.materializations_to_include:
                    non_table_upstream_dependencies.append(model_obj.name)

        if non_table_upstream_dependencies:
            raise DbtBouncerFailedCheckError(
                f"`{self.exposure.name}` is based on a model that is not a table: {non_table_upstream_dependencies}."
            )
