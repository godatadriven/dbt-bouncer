def test_top_level_directories(models):
    for model in models:
        top_level_dir = model["path"].split("/")[0]
        assert top_level_dir in [
            "staging",
            "intermediate",
            "marts",
        ], f"{model['unique_id']} is located in `{top_level_dir}`, this is not a valid top-level directory."
