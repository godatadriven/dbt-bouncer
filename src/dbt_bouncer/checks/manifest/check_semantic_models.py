from dbt_bouncer.check_decorator import check, fail


@check("check_semantic_model_based_on_non_public_models", iterate_over="semantic_model")
def check_semantic_model_based_on_non_public_models(semantic_model, ctx):
    """Semantic models should be based on public models only."""
    models_by_id = (
        ctx.models_by_unique_id
        if ctx.models_by_unique_id
        else {m.unique_id: m for m in ctx.models}
    )
    non_public_upstream_dependencies = []
    for model in getattr(semantic_model.depends_on, "nodes", []) or []:
        model_obj = models_by_id.get(model)
        if not model_obj:
            continue
        if (
            model_obj.resource_type == "model"
            and model_obj.package_name == semantic_model.package_name
            and model_obj.access
            and model_obj.access.value != "public"
        ):
            non_public_upstream_dependencies.append(model_obj.name)

    if non_public_upstream_dependencies:
        fail(
            f"Semantic model `{semantic_model.name}` is based on a model(s) that is not public: {non_public_upstream_dependencies}."
        )
