from typing import TYPE_CHECKING, Literal

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks._mixins import SemanticModelMixin

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
        DbtBouncerSemanticModelBase,
    )

from pydantic import Field

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class CheckSemanticModelOnNonPublicModels(SemanticModelMixin, BaseCheck):
    """Semantic models should be based on public models only.

    Receives:
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.
        semantic_model (DbtBouncerSemanticModelBase): The DbtBouncerSemanticModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the semantic model path (i.e the .yml file where the semantic model is configured). Semantic model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the semantic model path (i.e the .yml file where the semantic model is configured). Only semantic model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_semantic_model_based_on_non_public_models
        ```

    """

    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_semantic_model_based_on_non_public_models"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If semantic model is based on non-public models.

        """
        semantic_model = self._require_semantic_model()
        models_by_id = (
            self.models_by_unique_id
            if self.models_by_unique_id
            else {m.unique_id: m for m in self.models}
        )
        non_public_upstream_dependencies = []
        for model in getattr(semantic_model.depends_on, "nodes", []) or []:
            model_obj = models_by_id.get(model)
            if not model_obj:
                continue
            if (
                model_obj.resource_type == "model"
                and model_obj.package_name == semantic_model.package_name
                and model_obj.access
                and model_obj.access.value != "public"
            ):
                non_public_upstream_dependencies.append(model_obj.name)

        if non_public_upstream_dependencies:
            raise DbtBouncerFailedCheckError(
                f"Semantic model `{semantic_model.name}` is based on a model(s) that is not public: {non_public_upstream_dependencies}."
            )
