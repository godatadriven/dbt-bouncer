from typing import Any

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.types import RegexPattern
from dbt_bouncer.utils import clean_path_str, compile_pattern, get_clean_model_name


@check(code="LI001")
def check_lineage_permitted_upstream_models(
    model, ctx, *, package_name: str | None = None, upstream_path_pattern: RegexPattern
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
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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


def _require_database_or_schema_pattern(
    *, database_pattern: str | None, schema_pattern: str | None, **_: Any
) -> None:
    """Require at least one of `database_pattern` or `schema_pattern`.

    Raises:
        ValueError: If neither is supplied.

    """
    if database_pattern is None and schema_pattern is None:
        raise ValueError(
            "At least one of 'database_pattern' or 'schema_pattern' must be supplied."
        )


@check(code="LI004", validate=_require_database_or_schema_pattern)
def check_lineage_permitted_upstream_sources(
    model,
    ctx,
    *,
    database_pattern: RegexPattern | None = None,
    schema_pattern: RegexPattern | None = None,
):
    """Upstream sources must resolve to a database and/or schema that matches the provided patterns.

    !!! info "Rationale"

        In a multi-project (dbt mesh) setup, one project often reads the output of another project through sources. Each layer must only read from the layers of other projects that it is permitted to use, e.g. a marts model must only read from the marts layer of an upstream domain. The source path cannot express this, but the resolved database and schema of the source can.

    Parameters:
        database_pattern (str | None): Regexp pattern to match the resolved database of the upstream source(s).
        schema_pattern (str | None): Regexp pattern to match the resolved schema of the upstream source(s).

    Receives:
        model (ModelNode): The ModelNode object to check.
        sources (list[SourceNode]): List of SourceNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    !!! note

        At least one of `database_pattern` or `schema_pattern` must be supplied. If both are supplied, an upstream source must match both. A source with no database (e.g. on adapters without a database concept) does not match `database_pattern`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_lineage_permitted_upstream_sources
              include: ^models/marts|^models/consumption
              database_pattern: ^dmn
              schema_pattern: ^domain__
        ```

    """
    # In a live runner `ctx.sources` holds wrapped objects, so prefer the
    # unwrapped lookup. The unit-test harness only fills `ctx.sources`.
    sources_by_id = (
        ctx.sources_by_unique_id
        if ctx.sources_by_unique_id
        else {s.unique_id: s for s in ctx.sources}
    )
    # Map each supplied pattern to the SourceNode field it applies to.
    patterns = {
        field_name: compile_pattern(pattern.strip())
        for field_name, pattern in (
            ("database", database_pattern),
            ("schema_", schema_pattern),
        )
        if pattern is not None
    }
    not_permitted_upstream_sources = []
    for upstream_id in getattr(model.depends_on, "nodes", []) or []:
        source = sources_by_id.get(upstream_id)
        if source is None:
            continue
        if any(
            compiled.match(getattr(source, field_name, None) or "") is None
            for field_name, compiled in patterns.items()
        ):
            not_permitted_upstream_sources.append(
                f"{source.source_name}.{source.name} ({source.database}.{source.schema_})"
            )
    if not_permitted_upstream_sources:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` references upstream sources that are not permitted: {not_permitted_upstream_sources}."
        )


@check(code="LI002")
def check_lineage_seed_cannot_be_used(model):
    """Seed cannot be referenced in models with a path that matches the specified `include` config.

    !!! info "Rationale"

        Seeds are designed for small, static reference data (e.g. country codes, status mappings). Referencing seeds in intermediate or mart layers can indicate that data which should come from a source or staging model is instead being managed as a CSV file, making it harder to audit, version, and scale.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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


@check(code="LI003")
def check_lineage_source_cannot_be_used(model):
    """Sources cannot be referenced in models with a path that matches the specified `include` config.

    !!! info "Rationale"

        In a well-layered dbt project, raw sources should only be referenced from staging models. Allowing intermediate or mart models to query sources directly bypasses the staging layer, leads to duplicated transformation logic, and makes it harder to swap or rename sources without cascading changes across the project.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
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
