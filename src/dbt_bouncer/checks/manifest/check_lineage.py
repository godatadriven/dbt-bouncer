# mypy: disable-error-code="union-attr"

import re
from typing import TYPE_CHECKING, List, Literal

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.parsers import (
        DbtBouncerManifest,
        DbtBouncerModelBase,
    )

from pydantic import Field

from dbt_bouncer.utils import clean_path_str


class CheckLineagePermittedUpstreamModels(BaseCheck):
    """Upstream models must have a path that matches the provided `upstream_path_pattern`.

    Parameters:
        upstream_path_pattern (str): Regexp pattern to match the upstream model(s) path.

    Receives:
        manifest_obj (DbtBouncerManifest): The manifest object.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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

    manifest_obj: "DbtBouncerManifest" = Field(default=None)
    model: "DbtBouncerModelBase" = Field(default=None)
    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_lineage_permitted_upstream_models"]
    upstream_path_pattern: str

    def execute(self) -> None:
        """Execute the check."""
        upstream_models = [
            x
            for x in self.model.depends_on.nodes
            if x.split(".")[0] == "model"
            and x.split(".")[1] == self.manifest_obj.manifest.metadata.project_name
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
        assert not not_permitted_upstream_models, f"`{self.model.name}` references upstream models that are not permitted: {[m.split('.')[-1] for m in not_permitted_upstream_models]}."


class CheckLineageSeedCannotBeUsed(BaseCheck):
    """Seed cannot be referenced in models with a path that matches the specified `include` config.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_lineage_seed_cannot_be_used
              include: ^models/intermediate|^models/marts
        ```

    """

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_lineage_seed_cannot_be_used"]

    def execute(self) -> None:
        """Execute the check."""
        assert not [
            x for x in self.model.depends_on.nodes if x.split(".")[0] == "seed"
        ], f"`{self.model.name}` references a seed even though this is not permitted."


class CheckLineageSourceCannotBeUsed(BaseCheck):
    """Sources cannot be referenced in models with a path that matches the specified `include` config.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_lineage_source_cannot_be_used
              include: ^models/intermediate|^models/marts
        ```

    """

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_lineage_source_cannot_be_used"]

    def execute(self) -> None:
        """Execute the check."""
        assert not [
            x for x in self.model.depends_on.nodes if x.split(".")[0] == "source"
        ], f"`{self.model.name}` references a source even though this is not permitted."
