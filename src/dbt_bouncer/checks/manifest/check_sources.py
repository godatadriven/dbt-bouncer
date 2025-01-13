# mypy: disable-error-code="union-attr"

import re
from typing import TYPE_CHECKING, List, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_common import (
        DbtBouncerModelBase,
        DbtBouncerSourceBase,
    )
    from dbt_bouncer.checks.common import NestedDict


from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.utils import clean_path_str, find_missing_meta_keys


class CheckSourceDescriptionPopulated(BaseCheck):
    """Sources must have a populated description.

    Receives:
        source (DbtBouncerSourceBase): The DbtBouncerSourceBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_description_populated
        ```

    """

    name: Literal["check_source_description_populated"]
    source: "DbtBouncerSourceBase" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        assert len(self.source.description.strip()) > 4, (
            f"`{self.source.source_name}.{self.source.name}` does not have a populated description."
        )


class CheckSourceFreshnessPopulated(BaseCheck):
    """Sources must have a populated freshness.

    Receives:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_freshness_populated
        ```

    """

    name: Literal["check_source_freshness_populated"]
    source: "DbtBouncerSourceBase" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        error_msg = f"`{self.source.source_name}.{self.source.name}` does not have a populated freshness."
        assert self.source.freshness is not None, error_msg
        assert (
            self.source.freshness.error_after.count is not None
            and self.source.freshness.error_after.period is not None
        ) or (
            self.source.freshness.warn_after.count is not None
            and self.source.freshness.warn_after.period is not None
        ), error_msg


class CheckSourceHasMetaKeys(BaseCheck):
    """The `meta` config for sources must have the specified keys.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

    Receives:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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
    source: "DbtBouncerSourceBase" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        missing_keys = find_missing_meta_keys(
            meta_config=self.source.meta,
            required_keys=self.keys.model_dump(),
        )

        assert missing_keys == [], (
            f"`{self.source.source_name}.{self.source.name}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
        )


class CheckSourceHasTags(BaseCheck):
    """Sources must have the specified tags.

    Parameters:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.
        tags (List[str]): List of tags to check for.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_has_tags
              tags:
                - tag_1
                - tag_2
        ```

    """

    name: Literal["check_source_has_tags"]
    source: "DbtBouncerSourceBase" = Field(default=None)
    tags: List[str]

    def execute(self) -> None:
        """Execute the check."""
        missing_tags = [tag for tag in self.tags if tag not in self.source.tags]
        assert not missing_tags, (
            f"`{self.source.source_name}.{self.source.name}` is missing required tags: {missing_tags}."
        )


class CheckSourceLoaderPopulated(BaseCheck):
    """Sources must have a populated loader.

    Parameters:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_loader_populated
        ```

    """

    name: Literal["check_source_loader_populated"]
    source: "DbtBouncerSourceBase" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        assert self.source.loader != "", (
            f"`{self.source.source_name}.{self.source.name}` does not have a populated loader."
        )


class CheckSourceNames(BaseCheck):
    """Sources must have a name that matches the supplied regex.

    Parameters:
        source_name_pattern (str): Regexp the source name must match.

    Receives:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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
    source: "DbtBouncerSourceBase" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        assert (
            re.compile(self.source_name_pattern.strip()).match(self.source.name)
            is not None
        ), (
            f"`{self.source.source_name}.{self.source.name}` does not match the supplied regex `({self.source_name_pattern.strip()})`."
        )


class CheckSourceNotOrphaned(BaseCheck):
    """Sources must be referenced in at least one model.

    Receives:
        models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_not_orphaned
        ```

    """

    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_source_not_orphaned"]
    source: "DbtBouncerSourceBase" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        num_refs = sum(
            self.source.unique_id in model.depends_on.nodes for model in self.models
        )
        assert num_refs >= 1, (
            f"Source `{self.source.source_name}.{self.source.name}` is orphaned, i.e. not referenced by any model."
        )


class CheckSourcePropertyFileLocation(BaseCheck):
    """Source properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

    Receives:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_property_file_location
        ```

    """

    name: Literal["check_source_property_file_location"]
    source: "DbtBouncerSourceBase" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        path_cleaned = clean_path_str(self.source.original_file_path).replace(
            "models/staging", ""
        )
        expected_substring = "_".join(path_cleaned.split("/")[:-1])

        assert path_cleaned.split(
            "/",
        )[-1].startswith(
            "_",
        ), (
            f"The properties file for `{self.source.source_name}.{self.source.name}` (`{path_cleaned}`) does not start with an underscore."
        )
        assert expected_substring in path_cleaned, (
            f"The properties file for `{self.source.source_name}.{self.source.name}` (`{path_cleaned}`) does not contain the expected substring (`{expected_substring}`)."
        )
        assert path_cleaned.split(
            "/",
        )[-1].endswith(
            "__sources.yml",
        ), (
            f"The properties file for `{self.source.source_name}.{self.source.name}` (`{path_cleaned}`) does not end with `__sources.yml`."
        )


class CheckSourceUsedByModelsInSameDirectory(BaseCheck):
    """Sources can only be referenced by models that are located in the same directory where the source is defined.

    Parameters:
        models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_used_by_models_in_same_directory
        ```

    """

    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_source_used_by_models_in_same_directory"]
    source: "DbtBouncerSourceBase" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        reffed_models_not_in_same_dir = []
        for model in self.models:
            if (
                self.source.unique_id in model.depends_on.nodes
                and model.original_file_path.split("/")[:-1]
                != self.source.original_file_path.split("/")[:-1]
            ):
                reffed_models_not_in_same_dir.append(model.unique_id.split(".")[0])

        assert len(reffed_models_not_in_same_dir) == 0, (
            f"Source `{self.source.source_name}.{self.source.name}` is referenced by models defined in a different directory: {reffed_models_not_in_same_dir}"
        )


class CheckSourceUsedByOnlyOneModel(BaseCheck):
    """Each source can be referenced by a maximum of one model.

    Receives:
        models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_used_by_only_one_model
        ```

    """

    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_source_used_by_only_one_model"]
    source: "DbtBouncerSourceBase" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        num_refs = sum(
            self.source.unique_id in model.depends_on.nodes for model in self.models
        )
        assert num_refs <= 1, (
            f"Source `{self.source.source_name}.{self.source.name}` is referenced by more than one model."
        )
