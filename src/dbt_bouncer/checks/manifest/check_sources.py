# mypy: disable-error-code="union-attr"

import re
from typing import List, Literal, Union

import pytest
from _pytest.fixtures import TopRequest
from pydantic import Field

from dbt_bouncer.checks.common import NestedDict
from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.parsers import DbtBouncerModel, DbtBouncerSource
from dbt_bouncer.utils import bouncer_check, find_missing_meta_keys


class CheckSourceDescriptionPopulated(BaseCheck):
    name: Literal["check_source_description_populated"]


@pytest.mark.iterate_over_sources
@bouncer_check
def check_source_description_populated(
    request: TopRequest, source: Union[DbtBouncerSource, None] = None, **kwargs
) -> None:
    """
    Sources must have a populated description.

    Receives:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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


@pytest.mark.iterate_over_sources
@bouncer_check
def check_source_freshness_populated(
    request: TopRequest, source: Union[DbtBouncerSource, None] = None, **kwargs
) -> None:
    """
    Sources must have a populated freshness.

    Receives:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_freshness_populated
        ```
    """

    assert (
        source.freshness.error_after.count is not None
        and source.freshness.error_after.period is not None
    ) or (
        source.freshness.warn_after.count is not None
        and source.freshness.warn_after.period is not None
    ), f"`{source.source_name}.{source.name}` does not have a populated freshness."


class CheckSourceHasMetaKeys(BaseCheck):
    keys: NestedDict
    name: Literal["check_source_has_meta_keys"]


@pytest.mark.iterate_over_sources
@bouncer_check
def check_source_has_meta_keys(
    request,
    keys: Union[NestedDict, None] = None,
    source: Union[DbtBouncerSource, None] = None,
    **kwargs,
) -> None:
    """
    The `meta` config for sources must have the specified keys.

    Receives:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        keys (NestedDict): A list (that may contain sub-lists) of required keys.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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
    tags: List[str] = Field(
        default=[],
    )


@pytest.mark.iterate_over_sources
@bouncer_check
def check_source_has_tags(
    request: TopRequest,
    source: Union[DbtBouncerSource, None] = None,
    tags: Union[None, str] = None,
    **kwargs,
) -> None:
    """
    Sources must have the specified tags.

    Receives:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        source (DbtBouncerSource): The DbtBouncerSource object to check.
        tags (List[str]): List of tags to check for.

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


@pytest.mark.iterate_over_sources
@bouncer_check
def check_source_loader_populated(
    request: TopRequest, source: Union[DbtBouncerSource, None] = None, **kwargs
) -> None:
    """
    Sources must have a populated loader.

    Receives:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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


@pytest.mark.iterate_over_sources
@bouncer_check
def check_source_names(
    request: TopRequest,
    source: Union[DbtBouncerSource, None] = None,
    source_name_pattern: Union[None, str] = None,
    **kwargs,
) -> None:
    """
    Sources must have a name that matches the supplied regex.

    Receives:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        source (DbtBouncerSource): The DbtBouncerSource object to check.
        source_name_pattern (str): Regexp the source name must match.

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


@pytest.mark.iterate_over_sources
@bouncer_check
def check_source_not_orphaned(
    models: List[DbtBouncerModel],
    request: TopRequest,
    source: Union[DbtBouncerSource, None] = None,
    **kwargs,
) -> None:
    """
    Sources must be referenced in at least one model.

    Receives:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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


@pytest.mark.iterate_over_sources
@bouncer_check
def check_source_property_file_location(
    request: TopRequest, source: Union[DbtBouncerSource, None] = None, **kwargs
) -> None:
    """
    Source properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

    Receives:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_property_file_location
        ```
    """

    path_cleaned = source.path.replace("models/staging", "")
    expected_substring = "_".join(path_cleaned.split("/")[:-1])

    assert path_cleaned.split("/")[-1].startswith(
        "_"
    ), f"The properties file for `{source.source_name}.{source.name}` (`{path_cleaned}`) does not start with an underscore."
    assert (
        expected_substring in path_cleaned
    ), f"The properties file for `{source.source_name}.{source.name}` (`{path_cleaned}`) does not contain the expected substring (`{expected_substring}`)."
    assert path_cleaned.split("/")[-1].endswith(
        "__sources.yml"
    ), f"The properties file for `{source.source_name}.{source.name}` (`{path_cleaned}`) does not end with `__sources.yml`."


class CheckSourceUsedByModelsInSameDirectory(BaseCheck):
    name: Literal["check_source_used_by_models_in_same_directory"]


@pytest.mark.iterate_over_sources
@bouncer_check
def check_source_used_by_models_in_same_directory(
    models: List[DbtBouncerModel],
    request: TopRequest,
    source: Union[DbtBouncerSource, None] = None,
    **kwargs,
) -> None:
    """
    Sources can only be referenced by models that are located in the same directory where the source is defined.

    Receives:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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
            and model.path.split("/")[:-1] != source.path.split("/")[1:-1]
        ):
            reffed_models_not_in_same_dir.append(model.unique_id.split(".")[0])

    assert (
        len(reffed_models_not_in_same_dir) == 0
    ), f"Source `{source.source_name}.{source.name}` is referenced by models defined in a different directory: {reffed_models_not_in_same_dir}"


class CheckSourceUsedByOnlyOneModel(BaseCheck):
    name: Literal["check_source_used_by_only_one_model"]


@pytest.mark.iterate_over_sources
@bouncer_check
def check_source_used_by_only_one_model(
    models: List[DbtBouncerModel],
    request: TopRequest,
    source: Union[DbtBouncerSource, None] = None,
    **kwargs,
) -> None:
    """
    Each source can be referenced by a maximum of one model.

    Receives:
        exclude (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        source (DbtBouncerSource): The DbtBouncerSource object to check.

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
