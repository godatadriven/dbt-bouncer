import pytest

from dbt_bouncer.enums import Criteria
from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelHasTags:
    @pytest.mark.parametrize(
        ("tags", "criteria", "model", "check_fn"),
        [
            pytest.param(
                ["tag_1"],
                "all",
                {"tags": ["tag_1"]},
                check_passes,
                id="has_all_tags",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "all",
                {"tags": ["tag_1", "tag_2"]},
                check_passes,
                id="has_all_multiple_tags",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "any",
                {"tags": ["tag_1"]},
                check_passes,
                id="has_any_tag",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "one",
                {"tags": ["tag_1", "tag_3"]},
                check_passes,
                id="has_one_tag",
            ),
            pytest.param(
                ["tag_1"],
                "all",
                {"tags": ["tag_1", "tag_2", "tag_3"]},
                check_passes,
                id="has_all_tags_with_extra_tags",
            ),
            pytest.param(
                [],
                "all",
                {"tags": ["tag_1"]},
                check_passes,
                id="no_required_tags_vacuously_passes",
            ),
            pytest.param(
                ["tag_1"],
                "all",
                {"tags": []},
                check_fails,
                id="missing_tag",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "all",
                {"tags": ["tag_1"]},
                check_fails,
                id="missing_one_tag",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "any",
                {"tags": ["tag_3", "tag_4"]},
                check_fails,
                id="missing_any_tag",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "one",
                {"tags": ["tag_1", "tag_2"]},
                check_fails,
                id="has_more_than_one_tag",
            ),
            pytest.param(
                ["tag_1", "tag_2"],
                "one",
                {"tags": ["tag_3"]},
                check_fails,
                id="has_none_of_the_tags_with_criteria_one",
            ),
            pytest.param(
                [],
                "any",
                {"tags": ["tag_1"]},
                check_fails,
                id="no_required_tags_fails_criteria_any",
            ),
            pytest.param(
                [],
                "one",
                {"tags": ["tag_1"]},
                check_fails,
                id="no_required_tags_fails_criteria_one",
            ),
            pytest.param(
                ["Tag_1"],
                "all",
                {"tags": ["tag_1"]},
                check_fails,
                id="tag_case_mismatch",
            ),
            pytest.param(
                ["tag_1"],
                "all",
                {"tags": None},
                check_fails,
                id="tags_is_none",
            ),
            pytest.param(
                ["tag_1"],
                "all",
                {},
                check_fails,
                id="tags_key_absent",
            ),
        ],
    )
    def test_check_model_has_tags(self, tags, criteria, model, check_fn):
        check_fn("check_model_has_tags", model=model, tags=tags, criteria=criteria)

    @pytest.mark.parametrize(
        ("model", "check_fn"),
        [
            pytest.param({"tags": ["tag_1", "tag_2"]}, check_passes, id="has_all_tags"),
            pytest.param({"tags": ["tag_1"]}, check_fails, id="missing_one_tag"),
        ],
    )
    def test_check_model_has_tags_criteria_defaults_to_all(self, model, check_fn):
        check_fn("check_model_has_tags", model=model, tags=["tag_1", "tag_2"])

    @pytest.mark.parametrize(
        ("criteria", "model", "check_fn"),
        [
            pytest.param(
                Criteria.ALL,
                {"tags": ["tag_1", "tag_2"]},
                check_passes,
                id="criteria_all_enum",
            ),
            pytest.param(
                Criteria.ANY,
                {"tags": ["tag_2"]},
                check_passes,
                id="criteria_any_enum",
            ),
            pytest.param(
                Criteria.ONE,
                {"tags": ["tag_1"]},
                check_passes,
                id="criteria_one_enum",
            ),
            pytest.param(
                Criteria.ALL,
                {"tags": ["tag_1"]},
                check_fails,
                id="criteria_all_enum_missing_tag",
            ),
        ],
    )
    def test_check_model_has_tags_criteria_as_enum(self, criteria, model, check_fn):
        check_fn(
            "check_model_has_tags",
            model=model,
            tags=["tag_1", "tag_2"],
            criteria=criteria,
        )


class TestCheckModelHasTagsInvalidParam:
    def test_invalid_criteria_rejected(self):
        with pytest.raises(ValueError, match="'all', 'any' or 'one'"):
            check_passes(
                "check_model_has_tags",
                model={"tags": ["tag_1"]},
                tags=["tag_1"],
                criteria="alll",
            )
