import re

import pytest


@pytest.mark.iterate_over_models
def check_model_names(request, check_config=None, model=None):
    """
    Models must have a name that matches the supplied regex.
    """

    check_config = request.node.check_config if check_config is None else check_config
    model = request.node.model if model is None else model

    assert (
        re.compile(check_config["model_name_pattern"].strip()).match(model["name"]) is not None
    ), f"{model['unique_id']} does not match the supplied regex `({check_config['model_name_pattern'].strip()})`."


@pytest.mark.iterate_over_models
def check_populated_model_description(request, check_config=None, model=None):
    """
    Models must have a populated description.
    """

    model = request.node.model if model is None else model
    assert (
        len(model["description"].strip()) > 4
    ), f"{model['unique_id']} does not have a populated description."
