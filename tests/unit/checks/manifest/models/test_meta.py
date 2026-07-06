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
        ],
    )
    def test_fails(self, keys, model):
        check_fails("check_model_has_meta_keys", keys=keys, model=model)
