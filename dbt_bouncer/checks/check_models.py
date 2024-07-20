import pytest


@pytest.mark.iterate_over_models
def check_populated_model_description(request, check_config=None, model=None):
    """
    Models must have a populated description.
    """

    model = request.node.model if model is None else model
    assert (
        len(model["description"].strip()) > 4
    ), f"{model['unique_id']} does not have a populated description."
