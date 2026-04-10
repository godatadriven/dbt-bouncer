"""Checks related to model upstream dependencies and lineage."""

from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import get_clean_model_name


@check
def check_model_depends_on_macros(
    model, *, criteria: str = "all", required_macros: list[str]
):
    """Models must depend on the specified macros.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the model must depend on any, all, or exactly one of the specified macros. Default: `any`.
        required_macros: (list[str]): List of macros the model must depend on. All macros must specify a namespace, e.g. `dbt.is_incremental`.

    Receives:
        model (ModelNode): The ModelNode object to check.

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
    """Models cannot reference more than one source.

    !!! info "Rationale"

        A model that references multiple sources often signals that raw-layer joins are being performed too early, making the model harder to test, debug, and reuse. Enforcing single-source staging models encourages a clean DAG where each staging model maps 1:1 to a source table, and joins happen in downstream intermediate or mart models.

    Receives:
        model (ModelNode): The ModelNode object to check.

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
    """Models must have an exposure.

    Receives:
        exposures (list[ExposureNode]):  List of ExposureNode objects parsed from `manifest.json`.
        model (ModelNode): The ModelNode object to check.

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
    """Identify if models have no upstream dependencies as this likely indicates hard-coded tables references.

    !!! info "Rationale"

        A model with zero upstream dependencies is almost certainly using hard-coded table references (`FROM schema.table`) instead of `ref()` or `source()`. This breaks dbt's dependency graph, meaning the model won't run in the correct order, won't appear in lineage, and won't benefit from environment-aware compilation.

    Receives:
        model (ModelNode): The ModelNode object to check.

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
    max_chained_views: Annotated[int, Field(gt=0)] = 3,
    package_name: str | None = None,
):
    """Models cannot have more than the specified number of upstream dependents that are not tables.

    !!! info "Rationale"

        Long chains of views and ephemeral models force the data warehouse to execute deeply nested queries at read time, which degrades query performance and can hit platform-specific nesting limits. Capping chained views encourages materialising intermediate results, trading a small amount of storage for significantly faster query execution.

    Parameters:
        materializations_to_include (list[str] | None): List of materializations to include in the check.
        max_chained_views (int | None): The maximum number of upstream dependents that are not tables.

    Receives:
        model (ModelNode): The ModelNode object to check.
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

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
def check_model_max_fanout(
    model, ctx, *, max_downstream_models: Annotated[int, Field(gt=0)] = 3
):
    """Models cannot have more than the specified number of downstream models.

    Parameters:
        max_downstream_models (int | None): The maximum number of permitted downstream models.

    Receives:
        model (ModelNode): The ModelNode object to check.
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

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
    max_upstream_macros: Annotated[int, Field(gt=0)] = 5,
    max_upstream_models: Annotated[int, Field(gt=0)] = 5,
    max_upstream_sources: Annotated[int, Field(gt=0)] = 1,
):
    """Limit the number of upstream dependencies a model has.

    Parameters:
        max_upstream_macros (int | None): The maximum number of permitted upstream macros.
        max_upstream_models (int | None): The maximum number of permitted upstream models.
        max_upstream_sources (int | None): The maximum number of permitted upstream sources.

    Receives:
        model (ModelNode): The ModelNode object to check.

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
