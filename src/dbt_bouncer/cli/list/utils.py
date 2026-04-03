def category_key(check_class: type) -> str:
    """Return the display category name segment from the module path.

    Returns:
        str: The category name segment.

    """
    # e.g. "dbt_bouncer.checks.manifest.check_models" -> "manifest"
    parts = check_class.__module__.split(".")
    return parts[2] if len(parts) > 2 else "other"


# Fields inherited from BaseCheck that are not check-specific parameters.
base_fields = frozenset(
    {
        "catalog_node",
        "catalog_source",
        "description",
        "exclude",
        "exposure",
        "exposures_by_unique_id",
        "include",
        "index",
        "macro",
        "manifest_obj",
        "materialization",
        "model",
        "models_by_unique_id",
        "run_result",
        "seed",
        "semantic_model",
        "severity",
        "snapshot",
        "source",
        "sources_by_unique_id",
        "test",
        "tests_by_unique_id",
        "unit_test",
    }
)


def get_check_params(check_class: type) -> dict[str, str]:
    """Return configurable parameter names and their type annotations.

    Returns:
        dict[str, str]: Mapping of field name to type string.

    """
    params: dict[str, str] = {}
    for field_name, field_info in check_class.model_fields.items():  # type: ignore[attr-defined]
        if field_name in base_fields:
            continue
        annotation = field_info.annotation
        type_str = getattr(annotation, "__name__", None) or str(annotation)
        params[field_name] = type_str
    return params
