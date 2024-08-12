import re
from typing import Any, Dict, List, Literal, Optional, Union

import pytest
from pydantic import Field

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.utils import find_missing_meta_keys, get_check_inputs


class CheckSourceDescriptionPopulated(BaseCheck):
    name: Literal["check_source_description_populated"]


@pytest.mark.iterate_over_sources
def check_source_description_populated(request, source=None):
    """
    Sources must have a populated description.
    """

    source = get_check_inputs(source=source, request=request)["source"]

    assert (
        len(source["description"].strip()) > 4
    ), f"`{source['unique_id']}` does not have a populated description."


class CheckSourceFreshnessPopulated(BaseCheck):
    name: Literal["check_source_freshness_populated"]


@pytest.mark.iterate_over_sources
def check_source_freshness_populated(request, source=None):
    """
    Sources must have a populated freshness.
    """

    source = get_check_inputs(source=source, request=request)["source"]

    assert (
        source["freshness"]["error_after"]["count"] is not None
        and source["freshness"]["error_after"]["period"] is not None
    ) or (
        source["freshness"]["warn_after"]["count"] is not None
        and source["freshness"]["warn_after"]["period"] is not None
    ), f"`{source['unique_id']}` does not have a populated freshness."


class CheckSourceHasMetaKeys(BaseCheck):
    keys: Optional[Union[Dict[str, Any], List[Any]]]
    name: Literal["check_source_has_meta_keys"]


@pytest.mark.iterate_over_sources
def check_source_has_meta_keys(request, check_config=None, source=None) -> None:
    """
    The `meta` config for sources must have the specified keys.
    """

    input_vars = get_check_inputs(
        check_config=check_config,
        request=request,
        source=source,
    )
    check_config = input_vars["check_config"]
    source = input_vars["source"]

    missing_keys = find_missing_meta_keys(
        meta_config=source.get("meta"),
        required_keys=check_config["keys"],
    )
    assert (
        missing_keys == []
    ), f"`{source['unique_id']}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"


class CheckSourceHasTags(BaseCheck):
    name: Literal["check_source_has_tags"]
    tags: List[str] = Field(default=[], description="List of tags to check for.")


@pytest.mark.iterate_over_sources
def check_source_has_tags(request, check_config=None, source=None):
    """
    Sources must have the specified tags.
    """

    input_vars = get_check_inputs(check_config=check_config, source=source, request=request)
    source = input_vars["source"]
    tags = input_vars["check_config"]["tags"]

    missing_tags = [tag for tag in tags if tag not in source["tags"]]
    assert not missing_tags, f"`{source['unique_id']}` is missing required tags: {missing_tags}."


class CheckSourceLoaderPopulated(BaseCheck):
    name: Literal["check_source_loader_populated"]


@pytest.mark.iterate_over_sources
def check_source_loader_populated(request, source=None):
    """
    Sources must have a populated loader.
    """

    source = get_check_inputs(source=source, request=request)["source"]

    assert source["loader"] != "", f"`{source['unique_id']}` does not have a populated loader."


class CheckSourceNames(BaseCheck):
    name: Literal["check_source_names"]
    source_name_pattern: str = Field(description="Regexp the source name must match.")


@pytest.mark.iterate_over_sources
def check_source_names(request, check_config=None, source=None):
    """
    Sources must have a name that matches the supplied regex.
    """

    input_vars = get_check_inputs(check_config=check_config, source=source, request=request)
    check_config = input_vars["check_config"]
    source = input_vars["source"]

    assert (
        re.compile(check_config["source_name_pattern"].strip()).match(source["name"]) is not None
    ), f"`{source['unique_id'].split('.')[0]}` does not match the supplied regex `({check_config['source_name_pattern'].strip()})`."


class CheckSoureNotorphaned(BaseCheck):
    name: Literal["check_source_not_orphaned"]


@pytest.mark.iterate_over_sources
def check_source_not_orphaned(models, request, source=None) -> None:
    """
    Sources must be referenced in at least one model.
    """

    source = get_check_inputs(
        request=request,
        source=source,
    )["source"]

    num_refs = sum(source["unique_id"] in model["depends_on"]["nodes"] for model in models)
    assert (
        num_refs >= 1
    ), f"Source `{source['unique_id']}` is orphaned, i.e. not referenced by any model."


class CheckSourcePropertyFileLocation(BaseCheck):
    name: Literal["check_source_property_file_location"]


@pytest.mark.iterate_over_sources
def check_source_property_file_location(request, source=None):
    """
    Source properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).
    """

    source = get_check_inputs(source=source, request=request)["source"]

    path_cleaned = source["path"].replace("models/staging", "")
    expected_substring = "_".join(path_cleaned.split("/")[:-1])

    assert path_cleaned.split("/")[-1].startswith(
        "_"
    ), f"The properties file for `{source['unique_id']}` (`{path_cleaned}`) does not start with an underscore."
    assert (
        expected_substring in path_cleaned
    ), f"The properties file for `{source['unique_id']}` (`{path_cleaned}`) does not contain the expected substring (`{expected_substring}`)."
    assert path_cleaned.split("/")[-1].endswith(
        "__sources.yml"
    ), f"The properties file for `{source['unique_id']}` (`{path_cleaned}`) does not end with `__sources.yml`."


class CheckSourceUsedByModelsInSameDirectory(BaseCheck):
    name: Literal["check_source_used_by_models_in_same_directory"]


@pytest.mark.iterate_over_sources
def check_source_used_by_models_in_same_directory(models, request, source=None) -> None:
    """
    Sources can only be referenced by models that are located in the same directory where the source is defined.
    """

    source = get_check_inputs(
        request=request,
        source=source,
    )["source"]

    reffed_models_not_in_same_dir = []
    for model in models:
        if (
            source["unique_id"] in model["depends_on"]["nodes"]
            and model["path"].split("/")[:-1] != source["path"].split("/")[1:-1]
        ):
            reffed_models_not_in_same_dir.append(model["unique_id"].split(".")[0])

    assert (
        len(reffed_models_not_in_same_dir) == 0
    ), f"Source `{source['unique_id']}` is referenced by models defined in a different directory: {reffed_models_not_in_same_dir}"


class CheckSourceUsedByOnlyOneModel(BaseCheck):
    name: Literal["check_source_used_by_only_one_model"]


@pytest.mark.iterate_over_sources
def check_source_used_by_only_one_model(models, request, source=None) -> None:
    """
    Each source can be referenced by a maximum of one model.
    """

    source = get_check_inputs(
        request=request,
        source=source,
    )["source"]

    num_refs = sum(source["unique_id"] in model["depends_on"]["nodes"] for model in models)
    assert num_refs <= 1, f"Source `{source['unique_id']}` is referenced by more than one model."
