import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
        DbtBouncerSourceBase,
    )
    from dbt_bouncer.checks.common import NestedDict


from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import clean_path_str, find_missing_meta_keys


class CheckSourceDescriptionPopulated(BaseCheck):
    """Sources must have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

    Receives:
        source (DbtBouncerSourceBase): The DbtBouncerSourceBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_description_populated
        ```
        ```yaml
        manifest_checks:
            - name: check_source_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """

    min_description_length: int | None = Field(default=None)
    name: Literal["check_source_description_populated"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If description is not populated.

        """
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
        if not self._is_description_populated(
            self.source.description or "", self.min_description_length
        ):
            raise DbtBouncerFailedCheckError(
                f"`{self.source.source_name}.{self.source.name}` does not have a populated description."
            )


class CheckSourceFreshnessPopulated(BaseCheck):
    """Sources must have a populated freshness.

    Receives:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_freshness_populated
        ```

    """

    name: Literal["check_source_freshness_populated"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If freshness is not populated.

        """
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
        error_msg = f"`{self.source.source_name}.{self.source.name}` does not have a populated freshness."
        if self.source.freshness is None:
            raise DbtBouncerFailedCheckError(error_msg)
        has_error_after = (
            self.source.freshness.error_after
            and self.source.freshness.error_after.count is not None
            and self.source.freshness.error_after.period is not None
        )
        has_warn_after = (
            self.source.freshness.warn_after
            and self.source.freshness.warn_after.count is not None
            and self.source.freshness.warn_after.period is not None
        )

        if not (has_error_after or has_warn_after):
            raise DbtBouncerFailedCheckError(error_msg)


class CheckSourceHasMetaKeys(BaseCheck):
    """The `meta` config for sources must have the specified keys.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

    Receives:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_has_meta_keys
              keys:
                - contact:
                    - email
                    - slack
                - owner
        ```

    """

    keys: "NestedDict"
    name: Literal["check_source_has_meta_keys"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If required meta keys are missing.

        """
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
        missing_keys = find_missing_meta_keys(
            meta_config=self.source.meta,
            required_keys=self.keys.model_dump(),
        )

        if missing_keys != []:
            raise DbtBouncerFailedCheckError(
                f"`{self.source.source_name}.{self.source.name}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
            )


class CheckSourceHasTags(BaseCheck):
    """Sources must have the specified tags.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the source must have any, all, or exactly one of the specified tags. Default: `all`.
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.
        tags (list[str]): List of tags to check for.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_has_tags
              tags:
                - tag_1
                - tag_2
        ```

    """

    criteria: Literal["any", "all", "one"] = Field(default="all")
    name: Literal["check_source_has_tags"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)
    tags: list[str]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If source does not have required tags.

        """
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
        source_tags = self.source.tags or []
        if self.criteria == "any":
            if not any(tag in source_tags for tag in self.tags):
                raise DbtBouncerFailedCheckError(
                    f"`{self.source.source_name}.{self.source.name}` does not have any of the required tags: {self.tags}."
                )
        elif self.criteria == "all":
            missing_tags = set(self.tags) - set(source_tags)
            if missing_tags:
                raise DbtBouncerFailedCheckError(
                    f"`{self.source.source_name}.{self.source.name}` is missing required tags: {missing_tags}."
                )
        elif (
            self.criteria == "one" and sum(tag in source_tags for tag in self.tags) != 1
        ):
            raise DbtBouncerFailedCheckError(
                f"`{self.source.source_name}.{self.source.name}` must have exactly one of the required tags: {self.tags}."
            )


class CheckSourceLoaderPopulated(BaseCheck):
    """Sources must have a populated loader.

    Parameters:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_loader_populated
        ```

    """

    name: Literal["check_source_loader_populated"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If loader is not populated.

        """
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
        if self.source.loader == "":
            raise DbtBouncerFailedCheckError(
                f"`{self.source.source_name}.{self.source.name}` does not have a populated loader."
            )


class CheckSourceNames(BaseCheck):
    """Sources must have a name that matches the supplied regex.

    Parameters:
        source_name_pattern (str): Regexp the source name must match.

    Receives:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_names
              source_name_pattern: >
                ^[a-z0-9_]*$
        ```

    """

    name: Literal["check_source_names"]
    source_name_pattern: str
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If source name does not match regex.

        """
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
        if (
            re.compile(self.source_name_pattern.strip()).match(str(self.source.name))
            is None
        ):
            raise DbtBouncerFailedCheckError(
                f"`{self.source.source_name}.{self.source.name}` does not match the supplied regex `({self.source_name_pattern.strip()})`."
            )


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
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
        num_refs = sum(
            self.source.unique_id in getattr(model.depends_on, "nodes", [])
            for model in self.models
            if model.depends_on
        )
        if num_refs < 1:
            raise DbtBouncerFailedCheckError(
                f"Source `{self.source.source_name}.{self.source.name}` is orphaned, i.e. not referenced by any model."
            )


class CheckSourcePropertyFileLocation(BaseCheck):
    """Source properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

    Receives:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_property_file_location
        ```

    """

    name: Literal["check_source_property_file_location"]
    source: "DbtBouncerSourceBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If property file location is incorrect.

        """
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
        original_path = Path(clean_path_str(self.source.original_file_path))

        if (
            len(original_path.parts) > 2
            and original_path.parts[0] == "models"
            and original_path.parts[1] == "staging"
        ):
            subdir_parts = original_path.parent.parts[2:]
        else:
            subdir_parts = original_path.parent.parts

        expected_substring = "_" + "_".join(subdir_parts) if subdir_parts else ""
        properties_yml_name = original_path.name

        if not properties_yml_name.startswith("_"):
            raise DbtBouncerFailedCheckError(
                f"The properties file for `{self.source.source_name}.{self.source.name}` (`{properties_yml_name}`) does not start with an underscore."
            )
        if expected_substring not in properties_yml_name:
            raise DbtBouncerFailedCheckError(
                f"The properties file for `{self.source.source_name}.{self.source.name}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substring}`)."
            )
        if not properties_yml_name.endswith("__sources.yml"):
            raise DbtBouncerFailedCheckError(
                f"The properties file for `{self.source.source_name}.{self.source.name}` (`{properties_yml_name}`) does not end with `__sources.yml`."
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
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
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
        if self.source is None:
            raise DbtBouncerFailedCheckError("self.source is None")
        num_refs = sum(
            self.source.unique_id in getattr(model.depends_on, "nodes", [])
            for model in self.models
            if model.depends_on
        )
        if num_refs > 1:
            raise DbtBouncerFailedCheckError(
                f"Source `{self.source.source_name}.{self.source.name}` is referenced by more than one model."
            )
