"""Checks related to model versioning."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern


@check
def check_model_latest_version_specified(model):
    """Check that the `latest_version` attribute of the model is set.

    !!! info "Rationale"

        The `latest_version` attribute tells dbt which version of a model downstream consumers should default to when using `ref('model_name')` without specifying a version. Without it, dbt cannot resolve unversioned references, and consumers may inadvertently pin to an older version. Enforcing this attribute ensures the model versioning contract is fully specified.

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
            - name: check_model_latest_version_specified
              include: ^models/marts
        ```

    """
    if model.latest_version is None:
        fail(f"`{model.name}` does not have a specified `latest_version`.")


@check
def check_model_version_allowed(model, *, version_pattern: str):
    r"""Check that the version of the model matches the supplied regex pattern.

    !!! info "Rationale"

        Teams that use model versioning often enforce a convention for version identifiers — for example, numeric-only versions or semantic version strings. This check validates that version values conform to the team's chosen scheme, preventing arbitrary or malformed version strings from being introduced.

    Parameters:
        version_pattern (str): Regexp the version must match.

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
            - name: check_model_version_allowed
              include: ^models/marts
              version_pattern: >- # Versions must be numeric
                [0-9]\d*
            - name: check_model_version_allowed
              include: ^models/marts
              version_pattern: ^(stable|latest)$ # Version can be "stable" or "latest", nothing else is permitted
        ```

    """
    compiled = compile_pattern(version_pattern.strip())
    if model.version and (compiled.match(str(model.version)) is None):
        fail(
            f"Version `{model.version}` in `{model.name}` does not match the supplied regex `{version_pattern.strip()})`."
        )


@check
def check_model_version_pinned_in_ref(model, ctx):
    """Check that the version of the model is always specified in downstream nodes.

    !!! info "Rationale"

        When a versioned model is referenced without specifying a version, dbt resolves it to the `latest_version`, meaning a version bump can silently redirect all downstream consumers. Requiring explicit version pins in `ref()` calls ensures that version upgrades are a deliberate, reviewed change rather than an implicit one.

    Receives:
        manifest_obj (ManifestObject): The ManifestObject object parsed from `manifest.json`.
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
            - name: check_model_version_pinned_in_ref
              include: ^models/marts
        ```

    """
    manifest_obj = ctx.manifest_obj
    child_map = manifest_obj.manifest.child_map
    if child_map and model.unique_id in child_map:
        downstream_models = [
            x for x in child_map[model.unique_id] if x.startswith("model.")
        ]
    else:
        downstream_models = []

    downstream_models_with_unversioned_refs: list[str] = []
    for m in downstream_models:
        node = manifest_obj.manifest.nodes.get(m)
        refs = getattr(node, "refs", None)
        if node and refs and isinstance(refs, list):
            downstream_models_with_unversioned_refs.extend(
                m
                for ref in refs
                if getattr(ref, "name", None) == model.unique_id.split(".")[-1]
                and not getattr(ref, "version", None)
            )

    if downstream_models_with_unversioned_refs:
        fail(
            f"`{model.name}` is referenced without a pinned version in downstream models: {downstream_models_with_unversioned_refs}."
        )
