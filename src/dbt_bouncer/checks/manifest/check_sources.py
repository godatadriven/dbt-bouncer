# mypy: disable-error-code="union-attr"

import re
from typing import TYPE_CHECKING, List, Literal

if TYPE_CHECKING:
    from dbt_bouncer.checks.common import NestedDict
    from dbt_bouncer.parsers import DbtBouncerModel, DbtBouncerSource


from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.utils import find_missing_meta_keys


class CheckSourceDescriptionPopulated(BaseCheck):
    name: Literal["check_source_description_populated"]


def check_source_description_populated(source: "DbtBouncerSource", **kwargs) -> None:
    """Sources must have a populated description.

    Parameters:
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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
    assert (
        len(source.description.strip()) > 4
    ), f"`{source.source_name}.{source.name}` does not have a populated description."


class CheckSourceFreshnessPopulated(BaseCheck):
    name: Literal["check_source_freshness_populated"]


def check_source_freshness_populated(source: "DbtBouncerSource", **kwargs) -> None:
    """Sources must have a populated freshness.

    Parameters:
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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
    error_msg = (
        f"`{source.source_name}.{source.name}` does not have a populated freshness."
    )
    assert source.freshness is not None, error_msg
    assert (
        source.freshness.error_after.count is not None
        and source.freshness.error_after.period is not None
    ) or (
        source.freshness.warn_after.count is not None
        and source.freshness.warn_after.period is not None
    ), error_msg


class CheckSourceHasMetaKeys(BaseCheck):
    keys: "NestedDict"
    name: Literal["check_source_has_meta_keys"]


def check_source_has_meta_keys(
    keys: "NestedDict",
    source: "DbtBouncerSource",
    **kwargs,
) -> None:
    """The `meta` config for sources must have the specified keys.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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
    missing_keys = find_missing_meta_keys(
        meta_config=source.meta,
        required_keys=keys,
    )
    assert (
        missing_keys == []
    ), f"`{source.source_name}.{source.name}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"


class CheckSourceHasTags(BaseCheck):
    name: Literal["check_source_has_tags"]
    tags: List[str]


def check_source_has_tags(
    source: "DbtBouncerSource",
    tags: List[str],
    **kwargs,
) -> None:
    """Sources must have the specified tags.

    Parameters:
        source (DbtBouncerSource): The DbtBouncerSource object to check.
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
    missing_tags = [tag for tag in tags if tag not in source.tags]
    assert (
        not missing_tags
    ), f"`{source.source_name}.{source.name}` is missing required tags: {missing_tags}."


class CheckSourceLoaderPopulated(BaseCheck):
    name: Literal["check_source_loader_populated"]


def check_source_loader_populated(source: "DbtBouncerSource", **kwargs) -> None:
    """Sources must have a populated loader.

    Parameters:
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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
    assert (
        source.loader != ""
    ), f"`{source.source_name}.{source.name}` does not have a populated loader."


class CheckSourceNames(BaseCheck):
    name: Literal["check_source_names"]
    source_name_pattern: str


def check_source_names(
    source: "DbtBouncerSource",
    source_name_pattern: str,
    **kwargs,
) -> None:
    """Sources must have a name that matches the supplied regex.

    Parameters:
        source (DbtBouncerSource): The DbtBouncerSource object to check.
        source_name_pattern (str): Regexp the source name must match.

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
    assert (
        re.compile(source_name_pattern.strip()).match(source.name) is not None
    ), f"`{source.source_name}.{source.name}` does not match the supplied regex `({source_name_pattern.strip()})`."


class CheckSourceNotOrphaned(BaseCheck):
    name: Literal["check_source_not_orphaned"]


def check_source_not_orphaned(
    models: List["DbtBouncerModel"],
    source: "DbtBouncerSource",
    **kwargs,
) -> None:
    """Sources must be referenced in at least one model.

    Parameters:
        models (List[DbtBouncerModel]): List of DbtBouncerModel objects parsed from `manifest.json`.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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
    num_refs = sum(source.unique_id in model.depends_on.nodes for model in models)
    assert (
        num_refs >= 1
    ), f"Source `{source.source_name}.{source.name}` is orphaned, i.e. not referenced by any model."


class CheckSourcePropertyFileLocation(BaseCheck):
    name: Literal["check_source_property_file_location"]


def check_source_property_file_location(source: "DbtBouncerSource", **kwargs) -> None:
    """Source properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

    Parameters:
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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
    path_cleaned = source.original_file_path.replace("models/staging", "")
    expected_substring = "_".join(path_cleaned.split("/")[:-1])

    assert path_cleaned.split(
        "/",
    )[
        -1
    ].startswith(
        "_",
    ), f"The properties file for `{source.source_name}.{source.name}` (`{path_cleaned}`) does not start with an underscore."
    assert (
        expected_substring in path_cleaned
    ), f"The properties file for `{source.source_name}.{source.name}` (`{path_cleaned}`) does not contain the expected substring (`{expected_substring}`)."
    assert path_cleaned.split(
        "/",
    )[
        -1
    ].endswith(
        "__sources.yml",
    ), f"The properties file for `{source.source_name}.{source.name}` (`{path_cleaned}`) does not end with `__sources.yml`."


class CheckSourceUsedByModelsInSameDirectory(BaseCheck):
    name: Literal["check_source_used_by_models_in_same_directory"]


def check_source_used_by_models_in_same_directory(
    models: List["DbtBouncerModel"],
    source: "DbtBouncerSource",
    **kwargs,
) -> None:
    """Sources can only be referenced by models that are located in the same directory where the source is defined.

    Parameters:
        models (List[DbtBouncerModel]): List of DbtBouncerModel objects parsed from `manifest.json`.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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
    reffed_models_not_in_same_dir = []
    for model in models:
        if (
            source.unique_id in model.depends_on.nodes
            and model.original_file_path.split("/")[:-1]
            != source.original_file_path.split("/")[:-1]
        ):
            reffed_models_not_in_same_dir.append(model.unique_id.split(".")[0])

    assert (
        len(reffed_models_not_in_same_dir) == 0
    ), f"Source `{source.source_name}.{source.name}` is referenced by models defined in a different directory: {reffed_models_not_in_same_dir}"


class CheckSourceUsedByOnlyOneModel(BaseCheck):
    name: Literal["check_source_used_by_only_one_model"]


def check_source_used_by_only_one_model(
    models: List["DbtBouncerModel"],
    source: "DbtBouncerSource",
    **kwargs,
) -> None:
    """Each source can be referenced by a maximum of one model.

    Parameters:
        models (List[DbtBouncerModel]): List of DbtBouncerModel objects parsed from `manifest.json`.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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
    num_refs = sum(source.unique_id in model.depends_on.nodes for model in models)
    assert (
        num_refs <= 1
    ), f"Source `{source.source_name}.{source.name}` is referenced by more than one model."
