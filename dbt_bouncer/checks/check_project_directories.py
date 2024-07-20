import pytest


@pytest.mark.iterate_over_models
def check_top_level_directories(models, request, check_config=None):
    for model in models:
        top_level_dir = model["path"].split("/")[0]
        assert top_level_dir in [
            "staging",
            "intermediate",
            "marts",
        ], f"{model['unique_id']} is located in `{top_level_dir}`, this is not a valid top-level directory."
