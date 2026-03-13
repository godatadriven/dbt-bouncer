"""Checks related to model versioning."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern


@check
def check_model_latest_version_specified(model):
    """Check that the ``latest_version`` attribute of the model is set."""
    if model.latest_version is None:
        fail(f"`{model.name}` does not have a specified `latest_version`.")


@check
def check_model_version_allowed(model, *, version_pattern: str):
    """Check that the version of the model matches the supplied regex pattern."""
    compiled = compile_pattern(version_pattern.strip())
    if model.version and (compiled.match(str(model.version)) is None):
        fail(
            f"Version `{model.version}` in `{model.name}` does not match the supplied regex `{version_pattern.strip()})`."
        )


@check
def check_model_version_pinned_in_ref(model, ctx):
    """Check that the version of the model is always specified in downstream nodes."""
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
