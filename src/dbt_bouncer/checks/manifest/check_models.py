import re
from typing import List, Literal, Optional

import pytest
from pydantic import ConfigDict, Field

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.logger import logger
from dbt_bouncer.utils import get_check_inputs


class CheckModelAccess(BaseCheck):
    access: Literal["private", "protected", "public"]
    name: Literal["check_model_access"]


@pytest.mark.iterate_over_models
def check_model_access(request, check_config=None, model=None):
    """
    Models must have the specified access attribute. Requires dbt 1.7+.
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    check_config = input_vars["check_config"]
    model = input_vars["model"]

    assert (
        model["access"] == check_config["access"]
    ), f"{model['unique_id']} has `{model['access']}` access, it should have access `{check_config['access']}`."


class CheckModelDescriptionPopulated(BaseCheck):
    name: Literal["check_model_description_populated"]


@pytest.mark.iterate_over_models
def check_model_description_populated(request, model=None):
    """
    Models must have a populated description.
    """

    model = get_check_inputs(model=model, request=request)["model"]

    assert (
        len(model["description"].strip()) > 4
    ), f"{model['unique_id']} does not have a populated description."


class CheckModelCodeDoesNotContainRegexpPattern(BaseCheck):
    name: Literal["check_model_code_does_not_contain_regexp_pattern"]
    regexp_pattern: str = Field(
        description="The regexp pattern that should not be matched by the model code."
    )


@pytest.mark.iterate_over_models
def check_model_code_does_not_contain_regexp_pattern(request, check_config=None, model=None):
    """
    The raw code for a model must not match the specified regexp pattern.
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    check_config = input_vars["check_config"]
    model = input_vars["model"]

    assert (
        re.compile(check_config["regexp_pattern"].strip(), flags=re.DOTALL).match(
            model["raw_code"]
        )
        is None
    ), f"{model['unique_id']} contains a banned string: `{check_config['regexp_pattern'].strip()}`."


class CheckModelHasUniqueTest(BaseCheck):
    accepted_uniqueness_tests: Optional[List[str]] = Field(
        default=["expect_compound_columns_to_be_unique", "unique"],
        description="List of tests that are accepted as uniqueness tests. If not provided, defaults to `expect_compound_columns_to_be_unique` and `unique`.",
    )
    name: Literal["check_model_has_unique_test"]


@pytest.mark.iterate_over_models
def check_model_has_unique_test(request, tests, check_config=None, model=None):
    """
    Models must have a test for uniqueness of a column.
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    check_config = input_vars["check_config"]
    model = input_vars["model"]

    accepted_uniqueness_tests = check_config["accepted_uniqueness_tests"]
    logger.debug(f"{accepted_uniqueness_tests=}")

    num_unique_tests = sum(
        test["attached_node"] == model["unique_id"]
        and test["test_metadata"].get("name") in accepted_uniqueness_tests
        for test in tests
    )
    assert (
        num_unique_tests >= 1
    ), f"{model['unique_id']} does not have a test for uniqueness of a column."


class CheckModelNames(BaseCheck):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    name: Literal["check_model_names"]
    model_name_pattern: str = Field(description="Regexp the model name must match.")


@pytest.mark.iterate_over_models
def check_model_names(request, check_config=None, model=None):
    """
    Models must have a name that matches the supplied regex.
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    check_config = input_vars["check_config"]
    model = input_vars["model"]

    assert (
        re.compile(check_config["model_name_pattern"].strip()).match(model["name"]) is not None
    ), f"{model['unique_id']} does not match the supplied regex `({check_config['model_name_pattern'].strip()})`."
