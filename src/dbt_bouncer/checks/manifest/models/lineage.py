"""Checks related to model upstream dependencies and lineage."""

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerExposureBase,
        DbtBouncerManifest,
        DbtBouncerModelBase,
    )

from pydantic import ConfigDict, Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import get_clean_model_name


class CheckModelDependsOnMacros(BaseCheck):
    """Models must depend on the specified macros.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the model must depend on any, all, or exactly one of the specified macros. Default: `any`.
        required_macros: (list[str]): List of macros the model must depend on. All macros must specify a namespace, e.g. `dbt.is_incremental`.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_depends_on_macros
              required_macros:
                - dbt.is_incremental
            - name: check_model_depends_on_macros
              criteria: one
              required_macros:
                - my_package.sampler
                - my_package.sampling
        ```

    """

    criteria: Literal["any", "all", "one"] = Field(default="all")
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_depends_on_macros"]
    required_macros: list[str]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model does not depend on required macros.

        """
        self._require_model()
        upstream_macros = [
            (".").join(m.split(".")[1:])
            for m in getattr(self.model.depends_on, "macros", []) or []
        ]
        if self.criteria == "any":
            if not any(macro in upstream_macros for macro in self.required_macros):
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(self.model.unique_id)}` does not depend on any of the required macros: {self.required_macros}."
                )
        elif self.criteria == "all":
            missing_macros = [
                macro for macro in self.required_macros if macro not in upstream_macros
            ]
            if missing_macros:
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(self.model.unique_id)}` is missing required macros: {missing_macros}."
                )
        elif (
            self.criteria == "one"
            and sum(macro in upstream_macros for macro in self.required_macros) != 1
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` must depend on exactly one of the required macros: {self.required_macros}."
            )


class CheckModelDependsOnMultipleSources(BaseCheck):
    """Models cannot reference more than one source.

    Parameters:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_depends_on_multiple_sources
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_depends_on_multiple_sources"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model references more than one source.

        """
        self._require_model()
        num_reffed_sources = sum(
            x.split(".")[0] == "source"
            for x in getattr(self.model.depends_on, "nodes", []) or []
        )
        if num_reffed_sources > 1:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` references more than one source."
            )


class CheckModelHasExposure(BaseCheck):
    """Models must have an exposure.

    Receives:
        exposures (list[DbtBouncerExposureBase]):  List of DbtBouncerExposureBase objects parsed from `manifest.json`.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_exposure
              description: Ensure all marts are part of an exposure.
              include: ^models/marts
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    exposures: list["DbtBouncerExposureBase"] = Field(default=[])
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_exposure"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model does not have an exposure.

        """
        self._require_model()
        models_in_exposures = {
            node
            for e in self.exposures
            for node in (getattr(e.depends_on, "nodes", []) or [])
        }

        if self.model.unique_id not in models_in_exposures:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` does not have an associated exposure."
            )


class CheckModelHasNoUpstreamDependencies(BaseCheck):
    """Identify if models have no upstream dependencies as this likely indicates hard-coded tables references.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_no_upstream_dependencies
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_no_upstream_dependencies"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model has no upstream dependencies.

        """
        self._require_model()
        if (
            not self.model.depends_on
            or not self.model.depends_on.nodes
            or len(self.model.depends_on.nodes) <= 0
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has no upstream dependencies, this likely indicates hard-coded tables references."
            )


class CheckModelMaxChainedViews(BaseCheck):
    """Models cannot have more than the specified number of upstream dependents that are not tables.

    Parameters:
        materializations_to_include (list[str] | None): List of materializations to include in the check.
        max_chained_views (int | None): The maximum number of upstream dependents that are not tables.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_chained_views
        ```
        ```yaml
        manifest_checks:
            - name: check_model_max_chained_views
              materializations_to_include:
                - ephemeral
                - my_custom_materialization
                - view
              max_chained_views: 5
        ```

    """

    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    materializations_to_include: list[str] = Field(
        default=["ephemeral", "view"],
    )
    max_chained_views: int = Field(
        default=3,
    )
    model: "DbtBouncerModelBase | None" = Field(default=None)
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_model_max_chained_views"]
    package_name: str | None = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If max chained views exceeded.

        """
        self._require_model()
        self._require_manifest()

        models_by_id = (
            self.models_by_unique_id
            if self.models_by_unique_id
            else {m.unique_id: m for m in self.models}
        )

        def return_upstream_view_models(
            materializations,
            max_chained_views,
            model_unique_ids_to_check,
            package_name,
            depth=0,
        ):
            """Recursive function to return model unique_id's of upstream models that are views. Depth of recursion can be specified. If no models meet the criteria then an empty list is returned.

            Returns
            -
                list[str]: List of model unique_id's of upstream models that are views.

            """
            if depth == max_chained_views or model_unique_ids_to_check == []:
                return model_unique_ids_to_check

            relevant_upstream_models = []
            for model_id in model_unique_ids_to_check:
                model_obj = models_by_id.get(model_id)
                if model_obj is None:
                    continue
                upstream_nodes = (
                    list(getattr(model_obj.depends_on, "nodes", []) or [])
                    if model_obj.depends_on
                    else []
                )
                if upstream_nodes != []:
                    upstream_models = [
                        m
                        for m in upstream_nodes
                        if m.split(".")[0] == "model"
                        and m.split(".")[1] == package_name
                    ]
                    for i in upstream_models:
                        upstream_obj = models_by_id.get(i)
                        if (
                            upstream_obj
                            and upstream_obj.config
                            and upstream_obj.config.materialized in materializations
                        ):
                            relevant_upstream_models.append(i)

            depth += 1
            return return_upstream_view_models(
                materializations=materializations,
                max_chained_views=max_chained_views,
                model_unique_ids_to_check=relevant_upstream_models,
                package_name=package_name,
                depth=depth,
            )

        if (
            len(
                return_upstream_view_models(
                    materializations=self.materializations_to_include,
                    max_chained_views=self.max_chained_views,
                    model_unique_ids_to_check=[self.model.unique_id],
                    package_name=(
                        self.package_name
                        or self.manifest_obj.manifest.metadata.project_name
                    ),
                ),
            )
            != 0
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has more than {self.max_chained_views} upstream dependents that are not tables."
            )


class CheckModelMaxFanout(BaseCheck):
    """Models cannot have more than the specified number of downstream models.

    Parameters:
        max_downstream_models (int | None): The maximum number of permitted downstream models.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_fanout
              max_downstream_models: 2
        ```

    """

    max_downstream_models: int = Field(default=3)
    model: "DbtBouncerModelBase | None" = Field(default=None)
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_model_max_fanout"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If max fanout exceeded.

        """
        self._require_model()
        num_downstream_models = sum(
            self.model.unique_id
            in (getattr(m.depends_on, "nodes", []) if m.depends_on else [])
            for m in self.models
        )

        if num_downstream_models > self.max_downstream_models:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has {num_downstream_models} downstream models, which is more than the permitted maximum of {self.max_downstream_models}."
            )


class CheckModelMaxUpstreamDependencies(BaseCheck):
    """Limit the number of upstream dependencies a model has.

    Parameters:
        max_upstream_macros (int | None): The maximum number of permitted upstream macros.
        max_upstream_models (int | None): The maximum number of permitted upstream models.
        max_upstream_sources (int | None): The maximum number of permitted upstream sources.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_upstream_dependencies
              max_upstream_models: 3
        ```

    """

    max_upstream_macros: int = Field(
        default=5,
    )
    max_upstream_models: int = Field(
        default=5,
    )
    max_upstream_sources: int = Field(
        default=1,
    )
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_max_upstream_dependencies"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If max upstream dependencies exceeded.

        """
        self._require_model()
        depends_on = self.model.depends_on
        if depends_on:
            num_upstream_macros = len(list(getattr(depends_on, "macros", []) or []))
            nodes = getattr(depends_on, "nodes", []) or []
            num_upstream_models = len(
                [m for m in nodes if m.split(".")[0] == "model"],
            )
            num_upstream_sources = len(
                [m for m in nodes if m.split(".")[0] == "source"],
            )
        else:
            num_upstream_macros = 0
            num_upstream_models = 0
            num_upstream_sources = 0

        if num_upstream_macros > self.max_upstream_macros:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has {num_upstream_macros} upstream macros, which is more than the permitted maximum of {self.max_upstream_macros}."
            )
        if num_upstream_models > self.max_upstream_models:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has {num_upstream_models} upstream models, which is more than the permitted maximum of {self.max_upstream_models}."
            )
        if num_upstream_sources > self.max_upstream_sources:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has {num_upstream_sources} upstream sources, which is more than the permitted maximum of {self.max_upstream_sources}."
            )
