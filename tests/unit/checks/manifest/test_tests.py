import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckTestHasMetaKeys:
    @pytest.mark.parametrize(
        ("keys", "test_overrides", "check_fn"),
        [
            pytest.param(
                ["owner"],
                {"meta": {"owner": "team-finance"}},
                check_passes,
                id="has_required_key",
            ),
            pytest.param(
                ["owner", "maturity"],
                {"meta": {"owner": "team-finance", "maturity": "high"}},
                check_passes,
                id="has_all_required_keys",
            ),
            pytest.param(
                ["owner"],
                {"meta": {}},
                check_fails,
                id="missing_required_key",
            ),
            pytest.param(
                ["owner", "maturity"],
                {"meta": {"owner": "team-finance"}},
                check_fails,
                id="missing_one_of_required_keys",
            ),
        ],
    )
    def test_check_test_has_meta_keys(self, keys, test_overrides, check_fn):
        check_fn(
            "check_test_has_meta_keys",
            keys=keys,
            test=test_overrides,
        )


class TestCheckTestHasTags:
    @pytest.mark.parametrize(
        ("tags", "criteria", "test_overrides", "check_fn"),
        [
            pytest.param(
                ["critical"],
                "any",
                {"tags": ["critical"]},
                check_passes,
                id="has_required_tag_any",
            ),
            pytest.param(
                ["critical", "finance"],
                "any",
                {"tags": ["critical"]},
                check_passes,
                id="has_one_of_required_tags_any",
            ),
            pytest.param(
                ["critical", "finance"],
                "all",
                {"tags": ["critical", "finance"]},
                check_passes,
                id="has_all_required_tags",
            ),
            pytest.param(
                ["critical", "finance"],
                "one",
                {"tags": ["critical"]},
                check_passes,
                id="has_exactly_one_tag",
            ),
            pytest.param(
                ["critical"],
                "any",
                {"tags": []},
                check_fails,
                id="missing_tag_any",
            ),
            pytest.param(
                ["critical"],
                "any",
                {"tags": ["finance"]},
                check_fails,
                id="has_wrong_tag_any",
            ),
            pytest.param(
                ["critical", "finance"],
                "all",
                {"tags": ["critical"]},
                check_fails,
                id="missing_one_tag_all",
            ),
            pytest.param(
                ["critical", "finance"],
                "one",
                {"tags": ["critical", "finance"]},
                check_fails,
                id="has_two_tags_when_one_expected",
            ),
        ],
    )
    def test_check_test_has_tags(self, tags, criteria, test_overrides, check_fn):
        check_fn(
            "check_test_has_tags",
            criteria=criteria,
            tags=tags,
            test=test_overrides,
        )

    def test_check_test_has_tags_invalid_criteria(self):
        with pytest.raises(ValueError, match="'all', 'any' or 'one'"):
            check_passes(
                "check_test_has_tags",
                criteria="alll",
                tags=["critical"],
                test={"tags": ["critical"]},
            )


class TestCheckTestHasWhereConfig:
    @pytest.mark.parametrize(
        ("regexp_pattern", "test_overrides", "check_fn"),
        [
            pytest.param(
                None,
                {"config": {"where": "created_at >= '1970-01-01'"}},
                check_passes,
                id="where_present_no_pattern",
            ),
            pytest.param(
                None,
                {"config": {"where": "{{ partition_filter() }} >= 7"}},
                check_passes,
                id="where_jinja_present_no_pattern",
            ),
            pytest.param(
                None,
                {"config": {"where": None}},
                check_fails,
                id="where_none",
            ),
            pytest.param(
                None,
                {"config": {"where": "   "}},
                check_fails,
                id="where_blank",
            ),
            pytest.param(
                None,
                {"config": {}},
                check_fails,
                id="where_missing_from_config",
            ),
            pytest.param(
                None,
                {},
                check_fails,
                id="config_missing",
            ),
            pytest.param(
                ".*partition_filter.*",
                {"config": {"where": "{{ partition_filter() }} >= 7"}},
                check_passes,
                id="where_matches_pattern",
            ),
            pytest.param(
                ".*partition_filter.*",
                {"config": {"where": "created_at >= '1970-01-01'"}},
                check_fails,
                id="where_does_not_match_pattern",
            ),
            pytest.param(
                ".*partition_filter.*",
                {"config": {"where": None}},
                check_fails,
                id="where_none_with_pattern",
            ),
        ],
    )
    def test_check_test_has_where_config(
        self, regexp_pattern, test_overrides, check_fn
    ):
        check_fn(
            "check_test_has_where_config",
            regexp_pattern=regexp_pattern,
            test=test_overrides,
        )
