from dbt_bouncer.check_decorator import check, fail


@check
def check_exposure_based_on_model(
    exposure, *, maximum_number_of_models: int = 100, minimum_number_of_models: int = 1
):
    """Exposures should depend on a model.

    Parameters:
        maximum_number_of_models (int | None): The maximum number of models an exposure can depend on, defaults to 100.
        minimum_number_of_models (int | None): The minimum number of models an exposure can depend on, defaults to 1.

    Receives:
        exposure (ExposureNode): The ExposureNode object to check.

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

    Parameters:
        materializations_to_include (list[str] | None): List of materializations to include in the check.

    Receives:
        exposure (ExposureNode): The ExposureNode object to check.
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

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

    Receives:
        exposure (ExposureNode): The ExposureNode object to check.

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
            and model_obj.access.value != "public"
        ):
            non_public_upstream_dependencies.append(model_obj.name)

    if non_public_upstream_dependencies:
        fail(
            f"`{exposure.name}` is based on a model(s) that is not public: {non_public_upstream_dependencies}."
        )
