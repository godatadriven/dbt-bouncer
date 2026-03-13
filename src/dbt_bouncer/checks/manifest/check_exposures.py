from dbt_bouncer.check_decorator import check, fail


@check("check_exposure_based_on_model", iterate_over="exposure")
def check_exposure_based_on_model(
    exposure, *, maximum_number_of_models: int = 100, minimum_number_of_models: int = 1
):
    """Exposures should depend on a model."""
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


@check("check_exposure_based_on_view", iterate_over="exposure")
def check_exposure_based_on_view(
    exposure,
    ctx,
    *,
    materializations_to_include: list[str] = ["ephemeral", "view"],  # noqa: B006
):
    """Exposures should not be based on views."""
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


@check("check_exposure_based_on_non_public_models", iterate_over="exposure")
def check_exposure_based_on_non_public_models(exposure, ctx):
    """Exposures should be based on public models only."""
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
