"""Checks related to model upstream dependencies and lineage."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import get_clean_model_name


@check
def check_model_depends_on_macros(
    model, *, criteria: str = "all", required_macros: list[str]
):
    """Models must depend on the specified macros."""
    upstream_macros = [
        (".").join(m.split(".")[1:])
        for m in getattr(model.depends_on, "macros", []) or []
    ]
    if criteria == "any":
        if not any(macro in upstream_macros for macro in required_macros):
            fail(
                f"`{get_clean_model_name(model.unique_id)}` does not depend on any of the required macros: {required_macros}."
            )
    elif criteria == "all":
        missing_macros = [
            macro for macro in required_macros if macro not in upstream_macros
        ]
        if missing_macros:
            fail(
                f"`{get_clean_model_name(model.unique_id)}` is missing required macros: {missing_macros}."
            )
    elif (
        criteria == "one"
        and sum(macro in upstream_macros for macro in required_macros) != 1
    ):
        fail(
            f"`{get_clean_model_name(model.unique_id)}` must depend on exactly one of the required macros: {required_macros}."
        )


@check
def check_model_depends_on_multiple_sources(model):
    """Models cannot reference more than one source."""
    num_reffed_sources = sum(
        x.split(".")[0] == "source"
        for x in getattr(model.depends_on, "nodes", []) or []
    )
    if num_reffed_sources > 1:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` references more than one source."
        )


@check
def check_model_has_exposure(model, ctx):
    """Models must have an exposure."""
    models_in_exposures = {
        node
        for e in ctx.exposures
        for node in (getattr(e.depends_on, "nodes", []) or [])
    }

    if model.unique_id not in models_in_exposures:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` does not have an associated exposure."
        )


@check
def check_model_has_no_upstream_dependencies(model):
    """Identify if models have no upstream dependencies as this likely indicates hard-coded tables references."""
    if (
        not model.depends_on
        or not model.depends_on.nodes
        or len(model.depends_on.nodes) <= 0
    ):
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has no upstream dependencies, this likely indicates hard-coded tables references."
        )


@check
def check_model_max_chained_views(
    model,
    ctx,
    *,
    materializations_to_include: list[str] = ["ephemeral", "view"],  # noqa: B006
    max_chained_views: int = 3,
    package_name: str | None = None,
):
    """Models cannot have more than the specified number of upstream dependents that are not tables."""
    manifest_obj = ctx.manifest_obj

    models_by_id = (
        ctx.models_by_unique_id
        if ctx.models_by_unique_id
        else {m.unique_id: m for m in ctx.models}
    )

    def return_upstream_view_models(
        materializations, max_views, model_unique_ids_to_check, pkg_name, depth=0
    ):
        """Recursive function to return model unique_id's of upstream models that are views.

        Returns:
            list[str]: List of model unique_id's of upstream models that are views.

        """
        if depth == max_views or model_unique_ids_to_check == []:
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
                    if m.split(".")[0] == "model" and m.split(".")[1] == pkg_name
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
            max_views=max_views,
            model_unique_ids_to_check=relevant_upstream_models,
            pkg_name=pkg_name,
            depth=depth,
        )

    if (
        len(
            return_upstream_view_models(
                materializations=materializations_to_include,
                max_views=max_chained_views,
                model_unique_ids_to_check=[model.unique_id],
                pkg_name=(package_name or manifest_obj.manifest.metadata.project_name),
            )
        )
        != 0
    ):
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has more than {max_chained_views} upstream dependents that are not tables."
        )


@check
def check_model_max_fanout(model, ctx, *, max_downstream_models: int = 3):
    """Models cannot have more than the specified number of downstream models."""
    num_downstream_models = sum(
        model.unique_id in (getattr(m.depends_on, "nodes", []) if m.depends_on else [])
        for m in ctx.models
    )

    if num_downstream_models > max_downstream_models:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has {num_downstream_models} downstream models, which is more than the permitted maximum of {max_downstream_models}."
        )


@check
def check_model_max_upstream_dependencies(
    model,
    *,
    max_upstream_macros: int = 5,
    max_upstream_models: int = 5,
    max_upstream_sources: int = 1,
):
    """Limit the number of upstream dependencies a model has."""
    depends_on = model.depends_on
    if depends_on:
        num_upstream_macros = len(list(getattr(depends_on, "macros", []) or []))
        nodes = getattr(depends_on, "nodes", []) or []
        num_upstream_models = len([m for m in nodes if m.split(".")[0] == "model"])
        num_upstream_sources = len([m for m in nodes if m.split(".")[0] == "source"])
    else:
        num_upstream_macros = 0
        num_upstream_models = 0
        num_upstream_sources = 0

    if num_upstream_macros > max_upstream_macros:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has {num_upstream_macros} upstream macros, which is more than the permitted maximum of {max_upstream_macros}."
        )
    if num_upstream_models > max_upstream_models:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has {num_upstream_models} upstream models, which is more than the permitted maximum of {max_upstream_models}."
        )
    if num_upstream_sources > max_upstream_sources:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has {num_upstream_sources} upstream sources, which is more than the permitted maximum of {max_upstream_sources}."
        )
