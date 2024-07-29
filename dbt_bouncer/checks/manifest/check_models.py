import re
from typing import Literal

import pytest
from pydantic import ConfigDict, Field

from dbt_bouncer.config_validator_base import BaseCheck
from dbt_bouncer.utils import get_check_inputs


class CheckModelDescriptionPopulated(BaseCheck):
    name: Literal["check_model_description_populated"]


@pytest.mark.iterate_over_models
def check_model_description_populated(request, check_config=None, model=None):
    """
    Models must have a populated description.
    """

    model = get_check_inputs(model=model, request=request)["model"]

    assert (
        len(model["description"].strip()) > 4
    ), f"{model['unique_id']} does not have a populated description."


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
