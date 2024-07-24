import re

import pytest

from dbt_bouncer.logger import logger
from dbt_bouncer.utils import get_check_inputs


@pytest.mark.iterate_over_models
def check_model_names(request, check_config=None, model=None):
    """
    Models must have a name that matches the supplied regex.
    """

    check_config, _, model, _ = get_check_inputs(
        check_config=check_config, model=model, request=request
    )

    assert (
        re.compile(check_config["model_name_pattern"].strip()).match(model["name"]) is not None
    ), f"{model['unique_id']} does not match the supplied regex `({check_config['model_name_pattern'].strip()})`."


@pytest.mark.iterate_over_models
def check_populated_model_description(request, check_config=None, model=None):
    """
    Models must have a populated description.
    """

    check_config, _, model, _ = get_check_inputs(
        check_config=check_config, model=model, request=request
    )

    assert (
        len(model["description"].strip()) > 4
    ), f"{model['unique_id']} does not have a populated description."
