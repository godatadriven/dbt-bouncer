import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelHasLabelsKeys:
    @pytest.mark.parametrize(
        ("keys", "model"),
        [
            pytest.param(
                ["team"],
                {"config": {"labels": {"team": "finance"}}},
                id="has_key",
            ),
            pytest.param(
                ["team"],
                {"config": {"labels": {"env": "prod", "team": "analytics"}}},
                id="has_key_with_others",
            ),
            pytest.param(
                [{"team": ["subteam"]}],
                {"config": {"labels": {"team": {"subteam": "frontend"}}}},
                id="has_nested_key",
            ),
        ],
    )
    def test_passes(self, keys, model):
        check_passes("check_model_has_labels_keys", keys=keys, model=model)

    @pytest.mark.parametrize(
        ("keys", "model"),
        [
            pytest.param(
                ["team"],
                {"config": {"labels": {}}},
                id="missing_key",
            ),
            pytest.param(
                ["team"],
                {},
                id="no_labels_config",
            ),
            pytest.param(
                ["team"],
                {"config": {}},
                id="labels_key_absent",
            ),
            pytest.param(
                ["team"],
                {"config": {"labels": None}},
                id="labels_is_none",
            ),
            pytest.param(
                [{"team": ["subteam"]}],
                {"config": {"labels": {"team": {"other": "value"}}}},
                id="missing_nested_key",
            ),
        ],
    )
    def test_fails(self, keys, model):
        check_fails("check_model_has_labels_keys", keys=keys, model=model)


class TestCheckModelHasMetaKeys:
    @pytest.mark.parametrize(
        ("keys", "model"),
        [
            pytest.param(
                ["owner"],
                {"meta": {"owner": "Bob"}},
                id="has_key",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"maturity": "high", "owner": "Bob"}},
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
                id="has_nested_keys",
            ),
            pytest.param(
                ["key_1", "key_2"],
                {"meta": {"key_1": "abc", "key_2": ["a", "b", "c"]}},
                id="has_multiple_keys",
            ),
            # A required key is satisfied whenever it is present, regardless of
            # its value's shape or truthiness.
            pytest.param(
                ["owner"],
                {"meta": {"owner": None}},
                id="value_is_none",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"owner": ""}},
                id="value_is_empty_string",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"owner": []}},
                id="value_is_empty_list",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"owner": {}}},
                id="value_is_empty_dict",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"owner": {"name": "Bob"}}},
                id="value_is_dict_satisfies_plain_key",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"owner": list(range(11))}},
                id="value_is_long_list",
            ),
            # A digit-only key name is matched literally.
            pytest.param(
                ["2023"],
                {"meta": {"2023": "some_value"}},
                id="numeric_key_value",
            ),
            # A key whose name contains the path separator is matched literally.
            pytest.param(
                ["env>prod"],
                {"meta": {"env>prod": "some_value"}},
                id="key_contains_separator",
            ),
        ],
    )
    def test_passes(self, keys, model):
        check_passes("check_model_has_meta_keys", keys=keys, model=model)

    @pytest.mark.parametrize(
        ("keys", "model"),
        [
            pytest.param(
                ["owner"],
                {"meta": {}},
                id="missing_key",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"maturity": "high"}},
                id="missing_key_with_others",
            ),
            pytest.param(
                ["owner", {"name": ["first", "last"]}],
                {"meta": {"name": {"last": "Bobbington"}, "owner": "Bob"}},
                id="missing_nested_key",
            ),
            # A model with no `meta` config at all fails gracefully (rather than
            # raising) for any required key.
            pytest.param(
                ["owner"],
                {},
                id="meta_absent_entirely",
            ),
        ],
    )
    def test_fails(self, keys, model):
        check_fails("check_model_has_meta_keys", keys=keys, model=model)

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
                ["owner", "maturity"],
                {"meta": {}},
                r"\['owner', 'maturity'\]",
                id="multiple_keys",
            ),
        ],
    )
    def test_fails_message(self, keys, model, match):
        check_fails("check_model_has_meta_keys", keys=keys, model=model, match=match)
