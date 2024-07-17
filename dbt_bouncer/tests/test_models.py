def populated_model_description(models):
    """
    Models must have a populated description.
    """

    for model in models:
        assert (
            len(model["description"].strip()) > 4
        ), f"{model['unique_id']} does not have a populated description."


def test_populated_model_description(models):
    populated_model_description(models=models)
