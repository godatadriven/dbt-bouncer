from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.check_framework.exceptions import NestedDict
from dbt_bouncer.enums import ModelAccess
from dbt_bouncer.utils import find_missing_meta_keys, is_description_populated


@check
def check_exposure_based_on_model(
    exposure,
    *,
    maximum_number_of_models: Annotated[int, Field(gt=0)] = 100,
    minimum_number_of_models: Annotated[int, Field(gt=0)] = 1,
):
    """Exposures should depend on a model.

    !!! info "Rationale"

        Exposures document downstream consumers of dbt models — dashboards, ML models, and APIs. If an exposure references no models (or too many), it signals that the lineage metadata is incomplete or incorrect. Enforcing a model count range ensures each exposure is meaningfully connected to the data layer it represents, keeping documentation trustworthy.

    Parameters:
        maximum_number_of_models (int | None): The maximum number of models an exposure can depend on, defaults to 100.
        minimum_number_of_models (int | None): The minimum number of models an exposure can depend on, defaults to 1.

    Receives:
        exposure (ExposureNode): The ExposureNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match any pattern will be checked.
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
    depends_on = exposure.depends_on
    number_of_upstream_models = (
        len(getattr(depends_on, "nodes", []) or []) if depends_on else 0
    )

    if number_of_upstream_models < minimum_number_of_models:
        fail(
            f"`{exposure.name}` is based on less models ({number_of_upstream_models}) than the minimum permitted ({minimum_number_of_models})."
        )
    if number_of_upstream_models > maximum_number_of_models:
        fail(
            f"`{exposure.name}` is based on more models ({number_of_upstream_models}) than the maximum permitted ({maximum_number_of_models})."
        )


@check
def check_exposure_based_on_view(
    exposure,
    ctx,
    *,
    materializations_to_include: list[str] = ["ephemeral", "view"],  # noqa: B006
):
    """Exposures should not be based on views.

    !!! info "Rationale"

        Views and ephemeral models recompute their SQL every time they are queried. When a BI tool or downstream application queries an exposure built on a view, it triggers a full recomputation on every refresh, which can be slow and expensive at scale. Exposures should sit on top of materialised tables to ensure consistent, performant query times for end users.

    Parameters:
        materializations_to_include (list[str] | None): List of materializations to include in the check.

    Receives:
        exposure (ExposureNode): The ExposureNode object to check.
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match any pattern will be checked.
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
    models_by_id = (
        ctx.models_by_unique_id
        if ctx.models_by_unique_id
        else {m.unique_id: m for m in ctx.models}
    )
    non_table_upstream_dependencies = []
    for node_id in getattr(exposure.depends_on, "nodes", []) or []:
        model_obj = models_by_id.get(node_id)
        if (
            model_obj
            and model_obj.resource_type == "model"
            and model_obj.package_name == exposure.package_name
            and model_obj.config
            and model_obj.config.materialized in materializations_to_include
        ):
            non_table_upstream_dependencies.append(model_obj.name)

    if non_table_upstream_dependencies:
        fail(
            f"`{exposure.name}` is based on a model that is not a table: {non_table_upstream_dependencies}."
        )


@check
def check_exposure_based_on_non_public_models(exposure, ctx):
    """Exposures should be based on public models only.

    !!! info "Rationale"

        Public access in dbt signals that a model is stable, well-tested, and safe to depend on externally. Exposures that reference protected or private models create implicit dependencies on implementation details that may change without warning, increasing the risk of broken dashboards or pipelines when internal models are refactored.

    Receives:
        exposure (ExposureNode): The ExposureNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_exposure_based_on_non_public_models
        ```

    """
    models_by_id = (
        ctx.models_by_unique_id
        if ctx.models_by_unique_id
        else {m.unique_id: m for m in ctx.models}
    )
    non_public_upstream_dependencies = []
    for node_id in getattr(exposure.depends_on, "nodes", []) or []:
        model_obj = models_by_id.get(node_id)
        if (
            model_obj
            and model_obj.resource_type == "model"
            and model_obj.package_name == exposure.package_name
            and model_obj.access
            and model_obj.access.value != ModelAccess.PUBLIC
        ):
            non_public_upstream_dependencies.append(model_obj.name)

    if non_public_upstream_dependencies:
        fail(
            f"`{exposure.name}` is based on a model(s) that is not public: {non_public_upstream_dependencies}."
        )


@check
def check_exposure_description_populated(
    exposure, *, min_description_length: Annotated[int, Field(gt=0)] | None = None
):
    """Exposures must have a populated description.

    !!! info "Rationale"

        Exposures document downstream consumers of dbt models — dashboards, ML models, and APIs. Without descriptions, it is unclear what an exposure represents or who it serves, making it difficult for analysts and engineers to understand the lineage and governance of downstream consumers.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

    Receives:
        exposure (ExposureNode): The ExposureNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_exposure_description_populated
        ```
        ```yaml
        manifest_checks:
            - name: check_exposure_description_populated
              min_description_length: 25
        ```

    """
    if not is_description_populated(
        exposure.description or "", min_description_length or 4
    ):
        fail(f"`{exposure.name}` does not have a populated description.")


@check
def check_exposure_has_meta_keys(exposure, *, keys: NestedDict):
    """The `meta` config for exposures must have the specified keys.

    !!! info "Rationale"

        The `meta` config is a flexible, project-defined dictionary used to track ownership, maturity levels, and other governance attributes. Requiring specific keys on exposures ensures that governance information is consistently captured for all downstream consumers, enabling automated reporting and access-control workflows that depend on these attributes.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

    Receives:
        exposure (ExposureNode): The ExposureNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_exposure_has_meta_keys
              keys:
                - maturity
                - owner
        ```

    """
    missing_keys = find_missing_meta_keys(
        meta_config=exposure.meta or {}, required_keys=keys.model_dump()
    )
    if missing_keys:
        fail(
            f"`{exposure.name}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
        )


@check
def check_exposure_has_owner(
    exposure,
    *,
    required_fields: list[str] = ["email"],  # noqa: B006
):
    """Exposures must have owner information populated.

    !!! info "Rationale"

        Every exposure represents a downstream consumer (dashboard, ML model, API) that some person or team is responsible for. Without a populated owner, there is no clear point of contact when the underlying models change, when data quality issues arise, or when access needs to be reviewed. Enforcing required owner fields ensures accountability for every external dependency on dbt models.

    Parameters:
        required_fields (list[str]): List of owner fields that must be populated. Default: `["email"]`.

    Receives:
        exposure (ExposureNode): The ExposureNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Exposure paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the exposure path (i.e the .yml file where the exposure is configured). Only exposure paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_exposure_has_owner
        ```
        ```yaml
        manifest_checks:
            - name: check_exposure_has_owner
              required_fields:
                - email
                - name
        ```

    """
    if not exposure.owner:
        fail(f"`{exposure.name}` has no owner configured.")
    missing = [f for f in required_fields if not exposure.owner.get(f)]
    if missing:
        fail(f"`{exposure.name}` is missing required owner fields: {missing}.")
