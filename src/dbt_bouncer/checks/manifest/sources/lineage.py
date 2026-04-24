"""Checks related to source lineage and usage."""

from dbt_bouncer.check_framework.decorator import check, fail


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
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
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
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
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
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
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
