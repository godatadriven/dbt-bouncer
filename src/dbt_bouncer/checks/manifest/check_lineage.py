import re
from typing import TYPE_CHECKING, Literal

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        DbtBouncerModelBase,
    )

from pydantic import Field

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import clean_path_str, get_clean_model_name


class CheckLineagePermittedUpstreamModels(BaseCheck):
    """Upstream models must have a path that matches the provided `upstream_path_pattern`.

    Parameters:
        upstream_path_pattern (str): Regexp pattern to match the upstream model(s) path.

    Receives:
        manifest_obj (DbtBouncerManifest): The manifest object.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

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

    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    model: "DbtBouncerModelBase | None" = Field(default=None)
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_lineage_permitted_upstream_models"]
    package_name: str | None = Field(default=None)
    upstream_path_pattern: str

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If upstream models are not permitted.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if self.manifest_obj is None:
            raise DbtBouncerFailedCheckError("self.manifest_obj is None")
        upstream_models = [
            x
            for x in getattr(self.model.depends_on, "nodes", []) or []
            if x.split(".")[0] == "model"
            and x.split(".")[1]
            == (self.package_name or self.manifest_obj.manifest.metadata.project_name)
        ]
        not_permitted_upstream_models = [
            upstream_model
            for upstream_model in upstream_models
            if re.compile(self.upstream_path_pattern.strip()).match(
                clean_path_str(
                    next(
                        m for m in self.models if m.unique_id == upstream_model
                    ).original_file_path
                ),
            )
            is None
        ]
        if not_permitted_upstream_models:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` references upstream models that are not permitted: {[m.split('.')[-1] for m in not_permitted_upstream_models]}."
            )


class CheckLineageSeedCannotBeUsed(BaseCheck):
    """Seed cannot be referenced in models with a path that matches the specified `include` config.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_lineage_seed_cannot_be_used"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If seed is referenced.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if [
            x
            for x in getattr(self.model.depends_on, "nodes", []) or []
            if x.split(".")[0] == "seed"
        ]:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` references a seed even though this is not permitted."
            )


class CheckLineageSourceCannotBeUsed(BaseCheck):
    """Sources cannot be referenced in models with a path that matches the specified `include` config.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_lineage_source_cannot_be_used"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If source is referenced.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if [
            x
            for x in getattr(self.model.depends_on, "nodes", []) or []
            if x.split(".")[0] == "source"
        ]:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` references a source even though this is not permitted."
            )
