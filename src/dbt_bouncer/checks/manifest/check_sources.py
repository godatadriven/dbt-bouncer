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
    ), f"{source['unique_id']} is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"


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


class CheckSourceUsedByOnlyOneModel(BaseCheck):
    name: Literal["check_source_used_by_only_one_model"]


@pytest.mark.iterate_over_sources
def check_source_used_by_only_one_model(models, request, source=None) -> None:
    """
    Each source can be references by a maximum of one model.
    """

    source = get_check_inputs(
        request=request,
        source=source,
    )["source"]

    num_refs = sum(source["unique_id"] in model["depends_on"]["nodes"] for model in models)
    assert num_refs <= 1, f"Source `{source['unique_id']}` is referenced by more than one model."
