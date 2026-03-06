"""Checks related to source lineage and usage."""

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
        DbtBouncerSourceBase,
    )

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class CheckSourceNotOrphaned(BaseCheck):
    """Sources must be referenced in at least one model.

    Receives:
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

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

    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_source_not_orphaned"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If source is orphaned.

        """
        self._require_source()
        num_refs = sum(
            self.source.unique_id in getattr(model.depends_on, "nodes", [])
            for model in self.models
            if model.depends_on
        )
        if num_refs < 1:
            raise DbtBouncerFailedCheckError(
                f"Source `{self.source.source_name}.{self.source.name}` is orphaned, i.e. not referenced by any model."
            )


class CheckSourceUsedByModelsInSameDirectory(BaseCheck):
    """Sources can only be referenced by models that are located in the same directory where the source is defined.

    Parameters:
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

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

    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_source_used_by_models_in_same_directory"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If source is referenced by models in different directory.

        """
        self._require_source()
        reffed_models_not_in_same_dir = []
        for model in self.models:
            if (
                model.depends_on
                and self.source.unique_id in getattr(model.depends_on, "nodes", [])
                and model.original_file_path.split("/")[:-1]
                != self.source.original_file_path.split("/")[:-1]
            ):
                reffed_models_not_in_same_dir.append(model.name)

        if len(reffed_models_not_in_same_dir) != 0:
            raise DbtBouncerFailedCheckError(
                f"Source `{self.source.source_name}.{self.source.name}` is referenced by models defined in a different directory: {reffed_models_not_in_same_dir}"
            )


class CheckSourceUsedByOnlyOneModel(BaseCheck):
    """Each source can be referenced by a maximum of one model.

    Receives:
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

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

    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_source_used_by_only_one_model"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If source is referenced by more than one model.

        """
        self._require_source()
        num_refs = sum(
            self.source.unique_id in getattr(model.depends_on, "nodes", [])
            for model in self.models
            if model.depends_on
        )
        if num_refs > 1:
            raise DbtBouncerFailedCheckError(
                f"Source `{self.source.source_name}.{self.source.name}` is referenced by more than one model."
            )
