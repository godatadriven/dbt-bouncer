from dbt_bouncer.check_decorator import check, fail


@check
def check_semantic_model_based_on_non_public_models(semantic_model, ctx):
    """Semantic models should be based on public models only.

    !!! info "Rationale"

        Semantic models define the business-level metrics and dimensions that power tools like dbt's MetricFlow. If a semantic model references a protected or private model, it creates a hidden dependency on implementation details that can break when the underlying model is refactored. Basing semantic models on public models ensures they depend on a stable, intentionally-exposed interface.

    Receives:
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.
        semantic_model (SemanticModelNode): The SemanticModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the semantic model path (i.e the .yml file where the semantic model is configured). Semantic model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the semantic model path (i.e the .yml file where the semantic model is configured). Only semantic model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_semantic_model_based_on_non_public_models
        ```

    """
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
