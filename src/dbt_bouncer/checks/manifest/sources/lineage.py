"""Checks related to source lineage and usage."""

from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_framework.decorator import check, fail


@check
def check_duplicate_sources(source, ctx):
    """Sources must not share the same database relation.

    !!! info "Rationale"

        Two source definitions pointing at the same physical table cause ambiguous lineage and divergent staging logic. Different staging models may apply conflicting transformations to the same raw data, leading to inconsistent results. This check ensures every database relation is claimed by exactly one source definition.

    Receives:
        source (SourceNode): The SourceNode object to check.
        sources (list[SourceNode]): List of SourceNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Source paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Only source paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_duplicate_sources
        ```

    """

    def _node(s):
        """Return the SourceNode from either a SourceWrapper or a DictProxy.

        In a live runner ctx.sources contains SourceWrapper objects (SimpleNamespace
        with a .source attribute). In unit-test helpers ctx.sources items are
        DictProxy objects with direct field access. The None-fallback handles both.

        Returns:
            The unwrapped source node object with direct field access.

        """
        inner = getattr(s, "source", None)
        return inner if inner is not None else s

    src_node = _node(source)
    src_rel = (src_node.database, src_node.schema, src_node.identifier)
    dupes = []
    for s in ctx.sources:
        node = _node(s)
        if (
            node.unique_id != src_node.unique_id
            and (node.database, node.schema, node.identifier) == src_rel
        ):
            dupes.append(node.unique_id)
    if dupes:
        fail(
            f"Source `{source.source_name}.{source.name}` shares its relation ({src_rel}) with: {dupes}."
        )


@check
def check_source_min_downstream_models(
    source, ctx, *, min_number_of_models: Annotated[int, Field(gt=0)] = 1
):
    """Sources must be referenced by at least the specified number of models.

    !!! info "Rationale"

        Some sources are expected to feed multiple downstream models — for example, a raw events table that drives both a sessions model and a revenue model. Requiring a minimum number of consumers guards against accidental under-use and ensures that critical source data is fully exploited in the project's lineage.

        With the default `min_number_of_models` of 1 this check is equivalent to `check_source_not_orphaned`; prefer that check if you only need to assert a source is referenced at least once, and use this check when you want to require a higher minimum.

    Parameters:
        min_number_of_models (int): Minimum number of models that must reference the source. Must be greater than 0.

    Receives:
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.
        source (SourceNode): The SourceNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Source paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Only source paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_min_downstream_models
        ```
        ```yaml
        manifest_checks:
            - name: check_source_min_downstream_models
              min_number_of_models: 2
        ```

    """
    n = sum(
        source.unique_id in getattr(model.depends_on, "nodes", [])
        for model in ctx.models
        if model.depends_on
    )
    if n < min_number_of_models:
        fail(
            f"Source `{source.source_name}.{source.name}` is referenced by {n} model(s), fewer than the minimum {min_number_of_models}."
        )


@check
def check_source_not_orphaned(source, ctx):
    """Sources must be referenced in at least one model.

    !!! info "Rationale"

        An orphaned source — one that is declared in the project but never referenced by any model — is a maintenance liability. It adds noise to the project's source catalogue, may represent a data feed that is no longer needed, and can mislead new team members into thinking data is being used when it is not. This check keeps the source catalogue clean by flagging any source that has no downstream consumers, prompting teams to either wire it into the lineage graph or remove it.

    Receives:
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.
        source (SourceNode): The SourceNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Source paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Only source paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_not_orphaned
        ```

    """
    num_refs = sum(
        source.unique_id in getattr(model.depends_on, "nodes", [])
        for model in ctx.models
        if model.depends_on
    )
    if num_refs < 1:
        fail(
            f"Source `{source.source_name}.{source.name}` is orphaned, i.e. not referenced by any model."
        )


@check
def check_source_used_by_models_in_same_directory(source, ctx):
    """Sources can only be referenced by models that are located in the same directory where the source is defined.

    !!! info "Rationale"

        dbt's best-practice project structure places source definitions alongside the staging models that consume them. When a model in a distant directory references a source, it breaks this colocation principle, making it harder to understand which models own which sources and causing confusion about where raw-to-staged transformations live. Enforcing same-directory usage keeps source ownership explicit and the staging layer well-bounded.

    Receives:
        source (SourceNode): The SourceNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Source paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Only source paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_used_by_models_in_same_directory
        ```

    """
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


@check
def check_source_used_by_only_one_model(source, ctx):
    """Each source can be referenced by a maximum of one model.

    !!! info "Rationale"

        A common dbt best practice is to have a single staging model per source table, acting as the sole entry point that applies initial cleaning, renaming, and casting. When multiple models reference the same source directly, that cleaning logic is duplicated or diverges, leading to inconsistent representations of the same raw data across the project. Enforcing a single consumer per source encourages the staging-layer pattern and makes it clear where the authoritative transformation of each source lives.

    Receives:
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.
        source (SourceNode): The SourceNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Source paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Only source paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_used_by_only_one_model
        ```

    """
    num_refs = sum(
        source.unique_id in getattr(model.depends_on, "nodes", [])
        for model in ctx.models
        if model.depends_on
    )
    if num_refs > 1:
        fail(
            f"Source `{source.source_name}.{source.name}` is referenced by more than one model."
        )
