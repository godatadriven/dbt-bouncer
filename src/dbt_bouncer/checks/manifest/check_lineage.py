from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import clean_path_str, compile_pattern, get_clean_model_name


@check
def check_lineage_permitted_upstream_models(
    model, ctx, *, package_name: str | None = None, upstream_path_pattern: str
):
    """Upstream models must have a path that matches the provided `upstream_path_pattern`.

    !!! info "Rationale"

        A well-structured dbt project enforces clear layer boundaries — e.g. staging models only reference sources, intermediate models only reference staging or other intermediates, and marts only reference intermediates. Without this check, developers can inadvertently create cross-layer dependencies (a mart model directly referencing a staging model) that erode the project's modularity and make refactoring risky.

    Parameters:
        upstream_path_pattern (str): Regexp pattern to match the upstream model(s) path.

    Receives:
        manifest_obj (ManifestObject): The manifest object.
        model (ModelNode): The ModelNode object to check.
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_lineage_permitted_upstream_models
              include: ^models/staging
              upstream_path_pattern: $^
            - name: check_lineage_permitted_upstream_models
              include: ^models/intermediate
              upstream_path_pattern: ^models/staging|^models/intermediate
            - name: check_lineage_permitted_upstream_models
              include: ^models/marts
              upstream_path_pattern: ^models/staging|^models/intermediate
        ```

    """
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


@check
def check_lineage_seed_cannot_be_used(model):
    """Seed cannot be referenced in models with a path that matches the specified `include` config.

    !!! info "Rationale"

        Seeds are designed for small, static reference data (e.g. country codes, status mappings). Referencing seeds in intermediate or mart layers can indicate that data which should come from a source or staging model is instead being managed as a CSV file, making it harder to audit, version, and scale.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_lineage_seed_cannot_be_used
              include: ^models/intermediate|^models/marts
        ```

    """
    if [
        x
        for x in getattr(model.depends_on, "nodes", []) or []
        if x.split(".")[0] == "seed"
    ]:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` references a seed even though this is not permitted."
        )


@check
def check_lineage_source_cannot_be_used(model):
    """Sources cannot be referenced in models with a path that matches the specified `include` config.

    !!! info "Rationale"

        In a well-layered dbt project, raw sources should only be referenced from staging models. Allowing intermediate or mart models to query sources directly bypasses the staging layer, leads to duplicated transformation logic, and makes it harder to swap or rename sources without cascading changes across the project.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_lineage_source_cannot_be_used
              include: ^models/intermediate|^models/marts
        ```

    """
    if [
        x
        for x in getattr(model.depends_on, "nodes", []) or []
        if x.split(".")[0] == "source"
    ]:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` references a source even though this is not permitted."
        )
