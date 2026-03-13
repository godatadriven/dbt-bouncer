from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import clean_path_str, compile_pattern, get_clean_model_name


@check("check_lineage_permitted_upstream_models", iterate_over="model")
def check_lineage_permitted_upstream_models(
    model, ctx, *, package_name: str | None = None, upstream_path_pattern: str
):
    """Upstream models must have a path that matches the provided `upstream_path_pattern`."""
    compiled_upstream_path_pattern = compile_pattern(upstream_path_pattern.strip())
    manifest_obj = ctx.manifest_obj
    upstream_models = [
        x
        for x in getattr(model.depends_on, "nodes", []) or []
        if x.split(".")[0] == "model"
        and x.split(".")[1]
        == (package_name or manifest_obj.manifest.metadata.project_name)
    ]
    models_by_id = (
        ctx.models_by_unique_id
        if ctx.models_by_unique_id
        else {m.unique_id: m for m in ctx.models}
    )
    not_permitted_upstream_models = [
        upstream_model
        for upstream_model in upstream_models
        if upstream_model in models_by_id
        and compiled_upstream_path_pattern.match(
            clean_path_str(models_by_id[upstream_model].original_file_path)
        )
        is None
    ]
    if not_permitted_upstream_models:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` references upstream models that are not permitted: {[m.split('.')[-1] for m in not_permitted_upstream_models]}."
        )


@check("check_lineage_seed_cannot_be_used", iterate_over="model")
def check_lineage_seed_cannot_be_used(model):
    """Seed cannot be referenced in models with a path that matches the specified `include` config."""
    if [
        x
        for x in getattr(model.depends_on, "nodes", []) or []
        if x.split(".")[0] == "seed"
    ]:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` references a seed even though this is not permitted."
        )


@check("check_lineage_source_cannot_be_used", iterate_over="model")
def check_lineage_source_cannot_be_used(model):
    """Sources cannot be referenced in models with a path that matches the specified `include` config."""
    if [
        x
        for x in getattr(model.depends_on, "nodes", []) or []
        if x.split(".")[0] == "source"
    ]:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` references a source even though this is not permitted."
        )
