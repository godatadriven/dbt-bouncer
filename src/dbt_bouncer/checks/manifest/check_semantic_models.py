# mypy: disable-error-code="union-attr"


from typing import TYPE_CHECKING, List, Literal

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    import warnings

    from dbt_bouncer.parsers import DbtBouncerModel, DbtBouncerSemanticModel

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)


class CheckSemanticModelOnNonPublicModels(BaseCheck):
    name: Literal["check_semantic_model_based_on_non_public_models"]


def check_semantic_model_based_on_non_public_models(
    models: List["DbtBouncerModel"],
    semantic_model: "DbtBouncerSemanticModel",
    **kwargs,
) -> None:
    """Semantic models should be based on public models only.

    Parameters:
        models (List[DbtBouncerModel]): List of DbtBouncerModel objects parsed from `manifest.json`.
        semantic_model (DbtBouncerSemanticModel): The DbtBouncerSemanticModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the semantic model path (i.e the .yml file where the semantic model is configured). Semantic model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the semantic model path (i.e the .yml file where the semantic model is configured). Only semantic model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_semantic_model_based_on_non_public_models
        ```

    """
    non_public_upstream_dependencies = []
    for model in semantic_model.depends_on.nodes:
        if (
            model.split(".")[0] == "model"
            and model.split(".")[1] == semantic_model.package_name
        ):
            model = next(m for m in models if m.unique_id == model)
            if model.access.value != "public":
                non_public_upstream_dependencies.append(model.name)

    assert not non_public_upstream_dependencies, f"Semantic model `{semantic_model.name}` is based on a model(s) that is not public: {non_public_upstream_dependencies}."