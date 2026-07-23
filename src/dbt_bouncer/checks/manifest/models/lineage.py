"""Checks related to model upstream dependencies and lineage."""

from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.enums import Criteria
from dbt_bouncer.utils import get_clean_model_name


@check(code="MO028")
def check_model_depends_on_macros(
    model,
    *,
    criteria: Criteria = Criteria.ALL,
    required_macros: list[str],
):
    """Models must depend on the specified macros.

    !!! info "Rationale"

        Some teams mandate that certain model types always use shared macros for consistency — for example, requiring all incremental models to call `dbt.is_incremental()`. This check enforces those conventions, preventing models from re-implementing logic that is already standardised in a shared macro.

    Parameters:
        criteria (Literal["all", "any", "one"]): Whether the model must depend on any, all, or exactly one of the specified macros. Default: `all`.
        required_macros (list[str]): List of macros the model must depend on. All macros must specify a namespace, e.g. `dbt.is_incremental`.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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
    if criteria == Criteria.ANY:
        if not any(macro in upstream_macros for macro in required_macros):
            fail(
                f"`{get_clean_model_name(model.unique_id)}` does not depend on any of the required macros: {required_macros}."
            )
    elif criteria == Criteria.ALL:
        missing_macros = [
            macro for macro in required_macros if macro not in upstream_macros
        ]
        if missing_macros:
            fail(
                f"`{get_clean_model_name(model.unique_id)}` is missing required macros: {missing_macros}."
            )
    elif (
        criteria == Criteria.ONE
        and sum(macro in upstream_macros for macro in required_macros) != 1
    ):
        fail(
            f"`{get_clean_model_name(model.unique_id)}` must depend on exactly one of the required macros: {required_macros}."
        )


@check(code="MO029")
def check_model_depends_on_multiple_sources(model):
    """Models cannot reference more than one source.

    !!! info "Rationale"

        A model that references multiple sources often signals that raw-layer joins are being performed too early, making the model harder to test, debug, and reuse. Enforcing single-source staging models encourages a clean DAG where each staging model maps 1:1 to a source table, and joins happen in downstream intermediate or mart models.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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


@check(code="MO048")
def check_model_does_not_directly_join_to_source(model):
    """Models cannot reference a source and a model at the same time.

    !!! info "Rationale"

        A model that joins raw source data directly to an already-transformed model mixes two levels of abstraction in one place. The source side bypasses the staging layer, so the renaming, casting, and cleaning applied to every other consumer of that source is silently skipped. Routing every source through a staging model first keeps raw-data handling in exactly one place and makes the lineage graph read consistently from raw to curated.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_does_not_directly_join_to_source
        ```

    """
    # Only parents with a `model` resource type count as the curated side of the
    # join, matching dbt-project-evaluator's `fct_direct_join_to_source`. Seed and
    # snapshot parents are deliberately not counted.
    upstream_nodes = getattr(model.depends_on, "nodes", []) or []
    reffed_sources = [n for n in upstream_nodes if n.split(".")[0] == "source"]
    reffed_models = [n for n in upstream_nodes if n.split(".")[0] == "model"]

    if reffed_sources and reffed_models:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` references both a source ({sorted(reffed_sources)}) and a model ({sorted(reffed_models)}), i.e. it joins directly to a source instead of via a staging model."
        )


@check(code="MO049")
def check_model_does_not_rejoin_upstream_concepts(model, ctx):
    """Models cannot join back to an upstream concept that one of their other parents already depends on.

    !!! info "Rationale"

        A rejoin happens when a model reads from both a parent `B` and one of `B`'s own parents `A`. If `B` exists only to feed this model, then `A`'s columns are being pulled in twice by two different routes, and the two paths can drift apart as the logic evolves. Collapsing `B` into its only consumer — or having that consumer read solely from `B` — keeps each concept entering the model exactly once and makes the lineage graph honest about what depends on what.

    Receives:
        model (ModelNode): The ModelNode object to check.
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_does_not_rejoin_upstream_concepts
        ```

    """
    models_by_id = (
        ctx.models_by_unique_id
        if ctx.models_by_unique_id
        else {m.unique_id: m for m in ctx.models}
    )

    parents = set(getattr(model.depends_on, "nodes", []) or [])

    for parent_id in sorted(parents):
        parent = models_by_id.get(parent_id)
        if parent is None:
            continue

        shared_ancestors = set(getattr(parent.depends_on, "nodes", []) or []) & parents
        if not shared_ancestors:
            continue

        # Only a rejoin worth flagging when the intermediate parent exists solely
        # to feed this model; if it has other consumers it is a shared concept.
        if len(ctx.children_by_unique_id.get(parent_id, [])) == 1:
            fail(
                f"`{get_clean_model_name(model.unique_id)}` references `{get_clean_model_name(parent_id)}` and also references {sorted(get_clean_model_name(a) for a in shared_ancestors)}, which `{get_clean_model_name(parent_id)}` already depends on."
            )


@check(code="MO030")
def check_model_has_exposure(model, ctx):
    """Models must have an exposure.

    !!! info "Rationale"

        Exposures declare how dbt models are consumed by downstream tools such as dashboards, ML pipelines, or applications. Requiring mart models to be referenced in at least one exposure ensures that every curated output has a known consumer, making it easier to assess the impact of changes and avoid maintaining unused models.

    Receives:
        exposures (list[ExposureNode]):  List of ExposureNode objects parsed from `manifest.json`.
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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


@check(code="MO031")
def check_model_has_no_upstream_dependencies(model):
    """Identify if models have no upstream dependencies as this likely indicates hard-coded tables references.

    !!! info "Rationale"

        A model with zero upstream dependencies is almost certainly using hard-coded table references (`FROM schema.table`) instead of `ref()` or `source()`. This breaks dbt's dependency graph, meaning the model won't run in the correct order, won't appear in lineage, and won't benefit from environment-aware compilation.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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


@check(code="MO032")
def check_model_materialization_by_fanout(
    model,
    ctx,
    *,
    min_downstream_models: Annotated[int, Field(gt=0)] = 3,
    materializations: list[str] | None = None,
):
    """Heavily-reused models must use a durable materialization.

    !!! info "Rationale"

        A model consumed by many downstream models but materialized as a view
        re-executes its full query logic on every downstream build, multiplying
        warehouse compute and increasing overall pipeline latency. Hub models
        with high fanout should be persisted as tables or incremental models so
        that downstream builds read pre-computed results rather than re-running
        the same transformations repeatedly.

    Parameters:
        materializations (list[str] | None): Accepted durable materializations for models above the fanout threshold.
        min_downstream_models (int | None): The minimum number of downstream models that triggers this check.

    Receives:
        model (ModelNode): The ModelNode object to check.
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_materialization_by_fanout
        ```
        ```yaml
        manifest_checks:
            - name: check_model_materialization_by_fanout
              min_downstream_models: 5
              materializations:
                - incremental
                - table
        ```

    """
    materializations = materializations or ["incremental", "table"]
    num_downstream_models = len(ctx.children_by_unique_id.get(model.unique_id, []))
    materialized = model.config.materialized if model.config else None
    if (
        num_downstream_models >= min_downstream_models
        and materialized not in materializations
    ):
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has {num_downstream_models} downstream models but is materialized as `{materialized}`; expected one of {materializations}."
        )


@check(code="MO033")
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
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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


@check(code="MO034")
def check_model_max_fanout(
    model, ctx, *, max_downstream_models: Annotated[int, Field(gt=0)] = 3
):
    """Models cannot have more than the specified number of downstream models.

    !!! info "Rationale"

        A model with many direct downstream dependents becomes a high-impact change point — any modification to it requires testing and potentially breaking many consumers. Capping fanout encourages breaking widely-shared logic into more focused intermediate models, reducing the blast radius of changes.

    Parameters:
        max_downstream_models (int | None): The maximum number of permitted downstream models.

    Receives:
        model (ModelNode): The ModelNode object to check.
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_fanout
              max_downstream_models: 2
        ```

    """
    num_downstream_models = len(ctx.children_by_unique_id.get(model.unique_id, []))

    if num_downstream_models > max_downstream_models:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has {num_downstream_models} downstream models, which is more than the permitted maximum of {max_downstream_models}."
        )


@check(code="MO035")
def check_model_max_upstream_dependencies(
    model,
    *,
    max_upstream_macros: Annotated[int, Field(gt=0)] = 5,
    max_upstream_models: Annotated[int, Field(gt=0)] = 5,
    max_upstream_sources: Annotated[int, Field(gt=0)] = 1,
):
    """Limit the number of upstream dependencies a model has.

    !!! info "Rationale"

        A model that depends on too many upstream models, sources, or macros is likely doing too much in one place. Limiting upstream dependencies encourages splitting large transformations into smaller, testable units, which improves build times, reduces coupling, and makes the DAG easier to reason about.

    Parameters:
        max_upstream_macros (int | None): The maximum number of permitted upstream macros.
        max_upstream_models (int | None): The maximum number of permitted upstream models.
        max_upstream_sources (int | None): The maximum number of permitted upstream sources.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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
