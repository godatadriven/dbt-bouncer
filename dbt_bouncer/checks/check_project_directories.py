import pytest

from dbt_bouncer.utils import get_check_inputs


@pytest.mark.iterate_over_models
def check_top_level_directories(request, model=None):

    _, _, model, _ = get_check_inputs(model=model, request=request)
    top_level_dir = model["path"].split("/")[0]
    assert top_level_dir in [
        "staging",
        "intermediate",
        "marts",
    ], f"{model['unique_id']} is located in `{top_level_dir}`, this is not a valid top-level directory."
