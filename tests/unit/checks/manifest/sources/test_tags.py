import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceHasTags:
    @pytest.mark.parametrize(
        ("tags", "criteria", "source_tags"),
        [
            pytest.param(["tag_1"], "all", ["tag_1"], id="all_single_match"),
            pytest.param(["tag_1", "tag_2"], "any", ["tag_1"], id="any_one_match"),
            pytest.param(
                ["tag_1", "tag_2"], "one", ["tag_1", "tag_3"], id="one_exactly_one"
            ),
            pytest.param(["tag_1"], "one", ["tag_1"], id="one_single_match"),
        ],
    )
    def test_has_tags(self, tags, criteria, source_tags):
        check_passes(
            "check_source_has_tags",
            source={"tags": source_tags},
            tags=tags,
            criteria=criteria,
        )

    @pytest.mark.parametrize(
        ("tags", "criteria", "source_tags"),
        [
            pytest.param(["tag_1"], "all", [], id="all_empty_tags"),
            pytest.param(
                ["tag_1", "tag_2"], "any", ["tag_3", "tag_4"], id="any_no_match"
            ),
            pytest.param(
                ["tag_1", "tag_2"], "one", ["tag_1", "tag_2"], id="one_two_matches"
            ),
            pytest.param(["tag_1"], "one", [], id="one_empty_tags"),
        ],
    )
    def test_missing_tags(self, tags, criteria, source_tags):
        check_fails(
            "check_source_has_tags",
            source={"tags": source_tags},
            tags=tags,
            criteria=criteria,
        )
