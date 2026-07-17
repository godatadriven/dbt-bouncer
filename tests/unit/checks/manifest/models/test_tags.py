import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelHasTags:
    @pytest.mark.parametrize(
        ("tags", "criteria", "model"),
        [
            pytest.param(
                ["tag_1"],
                "all",
                {"tags": ["tag_1"]},
                id="has_all_tags",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "all",
                {"tags": ["tag_1", "tag_2"]},
                id="has_all_multiple_tags",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "any",
                {"tags": ["tag_1"]},
                id="has_any_tag",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "one",
                {"tags": ["tag_1", "tag_3"]},
                id="has_one_tag",
            ),
        ],
    )
    def test_passes(self, tags, criteria, model):
        check_passes("check_model_has_tags", model=model, tags=tags, criteria=criteria)

    @pytest.mark.parametrize(
        ("tags", "criteria", "model"),
        [
            pytest.param(
                ["tag_1"],
                "all",
                {"tags": []},
                id="missing_tag",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "all",
                {"tags": ["tag_1"]},
                id="missing_one_tag",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "any",
                {"tags": ["tag_3", "tag_4"]},
                id="missing_any_tag",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "one",
                {"tags": ["tag_1", "tag_2"]},
                id="has_more_than_one_tag",
            ),
        ],
    )
    def test_fails(self, tags, criteria, model):
        check_fails("check_model_has_tags", model=model, tags=tags, criteria=criteria)


class TestCheckModelHasTagsInvalidParam:
    def test_invalid_criteria_rejected(self):
        with pytest.raises(ValueError, match="'all', 'any' or 'one'"):
            check_passes(
                "check_model_has_tags",
                model={"tags": ["tag_1"]},
                tags=["tag_1"],
                criteria="alll",
            )
