"""Checks related to source lineage and usage."""

from dbt_bouncer.check_decorator import check, fail


@check("check_source_not_orphaned", iterate_over="source")
def check_source_not_orphaned(source, ctx):
    """Sources must be referenced in at least one model."""
    num_refs = sum(
        source.unique_id in getattr(model.depends_on, "nodes", [])
        for model in ctx.models
        if model.depends_on
    )
    if num_refs < 1:
        fail(
            f"Source `{source.source_name}.{source.name}` is orphaned, i.e. not referenced by any model."
        )


@check("check_source_used_by_models_in_same_directory", iterate_over="source")
def check_source_used_by_models_in_same_directory(source, ctx):
    """Sources can only be referenced by models that are located in the same directory where the source is defined."""
    reffed_models_not_in_same_dir = []
    for model in ctx.models:
        if (
            model.depends_on
            and source.unique_id in getattr(model.depends_on, "nodes", [])
            and model.original_file_path.split("/")[:-1]
            != source.original_file_path.split("/")[:-1]
        ):
            reffed_models_not_in_same_dir.append(model.name)

    if len(reffed_models_not_in_same_dir) != 0:
        fail(
            f"Source `{source.source_name}.{source.name}` is referenced by models defined in a different directory: {reffed_models_not_in_same_dir}"
        )


@check("check_source_used_by_only_one_model", iterate_over="source")
def check_source_used_by_only_one_model(source, ctx):
    """Each source can be referenced by a maximum of one model."""
    num_refs = sum(
        source.unique_id in getattr(model.depends_on, "nodes", [])
        for model in ctx.models
        if model.depends_on
    )
    if num_refs > 1:
        fail(
            f"Source `{source.source_name}.{source.name}` is referenced by more than one model."
        )
