import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelHasLabelsKeys:
    @pytest.mark.parametrize(
        ("keys", "model", "check_fn"),
        [
            pytest.param(
                ["team"],
                {"config": {"labels": {"team": "finance"}}},
                check_passes,
                id="has_key",
            ),
            pytest.param(
                ["team"],
                {"config": {"labels": {"env": "prod", "team": "analytics"}}},
                check_passes,
                id="has_key_with_others",
            ),
            pytest.param(
                [{"team": ["subteam"]}],
                {"config": {"labels": {"team": {"subteam": "frontend"}}}},
                check_passes,
                id="has_nested_key",
            ),
            pytest.param(
                [{"team": [{"subteam": ["lead"]}]}],
                {"config": {"labels": {"team": {"subteam": {"lead": "Bob"}}}}},
                check_passes,
                id="has_three_level_nested_key",
            ),
            # A required key is satisfied whenever it is present, regardless of
            # its value's truthiness.
            pytest.param(
                ["team"],
                {"config": {"labels": {"team": None}}},
                check_passes,
                id="value_is_none",
            ),
            pytest.param(
                [],
                {"config": {"labels": {}}},
                check_passes,
                id="no_required_keys_vacuously_passes",
            ),
            pytest.param(
                ["team"],
                {"config": {"labels": {}}},
                check_fails,
                id="missing_key",
            ),
            pytest.param(
                ["team"],
                {},
                check_fails,
                id="no_labels_config",
            ),
            pytest.param(
                ["team"],
                {"config": {}},
                check_fails,
                id="labels_key_absent",
            ),
            pytest.param(
                ["team"],
                {"config": {"labels": None}},
                check_fails,
                id="labels_is_none",
            ),
            pytest.param(
                [{"team": ["subteam"]}],
                {"config": {"labels": {"team": {"other": "value"}}}},
                check_fails,
                id="missing_nested_key",
            ),
            pytest.param(
                ["Team"],
                {"config": {"labels": {"team": "finance"}}},
                check_fails,
                id="key_case_mismatch",
            ),
        ],
    )
    def test_check_model_has_labels_keys(self, keys, model, check_fn):
        check_fn("check_model_has_labels_keys", keys=keys, model=model)

    @pytest.mark.parametrize(
        ("keys", "model", "match"),
        [
            pytest.param(
                ["team"],
                {"config": {"labels": {}}},
                r"\['team'\]",
                id="top_level_key",
            ),
            pytest.param(
                [{"team": ["subteam"]}],
                {"config": {"labels": {"team": {"other": "value"}}}},
                r"\['team>subteam'\]",
                id="nested_key",
            ),
        ],
    )
    def test_check_model_has_labels_keys_message(self, keys, model, match):
        check_fails("check_model_has_labels_keys", keys=keys, model=model, match=match)


class TestCheckModelHasMetaKeys:
    @pytest.mark.parametrize(
        ("keys", "model", "check_fn"),
        [
            pytest.param(
                ["owner"],
                {"meta": {"owner": "Bob"}},
                check_passes,
                id="has_key",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"maturity": "high", "owner": "Bob"}},
                check_passes,
                id="has_key_with_others",
            ),
            pytest.param(
                ["owner", {"name": ["first", "last"]}],
                {
                    "meta": {
                        "name": {"first": "Bob", "last": "Bobbington"},
                        "owner": "Bob",
                    },
                },
                check_passes,
                id="has_nested_keys",
            ),
            pytest.param(
                [{"a": [{"b": ["c"]}]}],
                {"meta": {"a": {"b": {"c": "value"}}}},
                check_passes,
                id="has_three_level_nested_keys",
            ),
            pytest.param(
                ["key_1", "key_2"],
                {"meta": {"key_1": "abc", "key_2": ["a", "b", "c"]}},
                check_passes,
                id="has_multiple_keys",
            ),
            # A required key is satisfied whenever it is present, regardless of
            # its value's shape or truthiness.
            pytest.param(
                ["owner"],
                {"meta": {"owner": None}},
                check_passes,
                id="value_is_none",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"owner": ""}},
                check_passes,
                id="value_is_empty_string",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"owner": []}},
                check_passes,
                id="value_is_empty_list",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"owner": {}}},
                check_passes,
                id="value_is_empty_dict",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"owner": {"name": "Bob"}}},
                check_passes,
                id="value_is_dict_satisfies_plain_key",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"owner": list(range(11))}},
                check_passes,
                id="value_is_long_list",
            ),
            # A digit-only key name is matched literally.
            pytest.param(
                ["2023"],
                {"meta": {"2023": "some_value"}},
                check_passes,
                id="numeric_key_value",
            ),
            # A key whose name contains the path separator is matched literally.
            pytest.param(
                ["env>prod"],
                {"meta": {"env>prod": "some_value"}},
                check_passes,
                id="key_contains_separator",
            ),
            pytest.param(
                [],
                {"meta": {}},
                check_passes,
                id="no_required_keys_vacuously_passes",
            ),
            pytest.param(
                ["owner"],
                {"meta": {}},
                check_fails,
                id="missing_key",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"maturity": "high"}},
                check_fails,
                id="missing_key_with_others",
            ),
            pytest.param(
                ["owner", {"name": ["first", "last"]}],
                {"meta": {"name": {"last": "Bobbington"}, "owner": "Bob"}},
                check_fails,
                id="missing_nested_key",
            ),
            pytest.param(
                [{"a": [{"b": ["c"]}]}],
                {"meta": {"a": {"b": {"d": "value"}}}},
                check_fails,
                id="missing_three_level_nested_key",
            ),
            pytest.param(
                ["Owner"],
                {"meta": {"owner": "Bob"}},
                check_fails,
                id="key_case_mismatch",
            ),
            # A model with no `meta` config at all fails gracefully (rather than
            # raising) for any required key.
            pytest.param(
                ["owner"],
                {},
                check_fails,
                id="meta_absent_entirely",
            ),
        ],
    )
    def test_check_model_has_meta_keys(self, keys, model, check_fn):
        check_fn("check_model_has_meta_keys", keys=keys, model=model)

    @pytest.mark.parametrize(
        ("keys", "model", "match"),
        [
            pytest.param(
                ["owner"],
                {"meta": {}},
                r"\['owner'\]",
                id="top_level_key",
            ),
            pytest.param(
                [{"name": ["first"]}],
                {"meta": {"name": {"last": "Bobbington"}}},
                r"\['name>first'\]",
                id="nested_key",
            ),
            pytest.param(
                [{"a": [{"b": ["c"]}]}],
                {"meta": {"a": {"b": {"d": "value"}}}},
                r"\['a>b>c'\]",
                id="three_level_nested_key",
            ),
            pytest.param(
                ["owner", "maturity"],
                {"meta": {}},
                r"\['owner', 'maturity'\]",
                id="multiple_keys",
            ),
        ],
    )
    def test_fails_message(self, keys, model, match):
        check_fails("check_model_has_meta_keys", keys=keys, model=model, match=match)
