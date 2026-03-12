"""Checks related to source lineage and usage."""

from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class CheckSourceNotOrphaned(BaseCheck):
    """Sources must be referenced in at least one model.

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

    name: Literal["check_source_not_orphaned"]
    source: Any | None = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If source is orphaned.

        """
        source = self._require_source()
        num_refs = sum(
            source.unique_id in getattr(model.depends_on, "nodes", [])
            for model in self._ctx.models
            if model.depends_on
        )
        if num_refs < 1:
            raise DbtBouncerFailedCheckError(
                f"Source `{source.source_name}.{source.name}` is orphaned, i.e. not referenced by any model."
            )


class CheckSourceUsedByModelsInSameDirectory(BaseCheck):
    """Sources can only be referenced by models that are located in the same directory where the source is defined.

    Parameters:
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
            - name: check_source_used_by_models_in_same_directory
        ```

    """

    name: Literal["check_source_used_by_models_in_same_directory"]
    source: Any | None = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If source is referenced by models in different directory.

        """
        source = self._require_source()
        reffed_models_not_in_same_dir = []
        for model in self._ctx.models:
            if (
                model.depends_on
                and source.unique_id in getattr(model.depends_on, "nodes", [])
                and model.original_file_path.split("/")[:-1]
                != source.original_file_path.split("/")[:-1]
            ):
                reffed_models_not_in_same_dir.append(model.name)

        if len(reffed_models_not_in_same_dir) != 0:
            raise DbtBouncerFailedCheckError(
                f"Source `{source.source_name}.{source.name}` is referenced by models defined in a different directory: {reffed_models_not_in_same_dir}"
            )


class CheckSourceUsedByOnlyOneModel(BaseCheck):
    """Each source can be referenced by a maximum of one model.

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

    name: Literal["check_source_used_by_only_one_model"]
    source: Any | None = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If source is referenced by more than one model.

        """
        source = self._require_source()
        num_refs = sum(
            source.unique_id in getattr(model.depends_on, "nodes", [])
            for model in self._ctx.models
            if model.depends_on
        )
        if num_refs > 1:
            raise DbtBouncerFailedCheckError(
                f"Source `{source.source_name}.{source.name}` is referenced by more than one model."
            )
