import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceHasMetaKeys:
    @pytest.mark.parametrize(
        ("keys", "meta"),
        [
            pytest.param(
                ["owner"],
                {"owner": "Bob"},
                id="single_key",
            ),
            pytest.param(
                ["owner"],
                {"maturity": "high", "owner": "Bob"},
                id="extra_keys",
            ),
            pytest.param(
                ["owner", {"name": ["first", "last"]}],
                {"name": {"first": "Bob", "last": "Bobbington"}, "owner": "Bob"},
                id="nested_keys",
            ),
        ],
    )
    def test_has_meta_keys(self, keys, meta):
        check_passes(
            "check_source_has_meta_keys",
            source={"meta": meta},
            keys=keys,
        )

    @pytest.mark.parametrize(
        ("keys", "meta"),
        [
            pytest.param(
                ["owner"],
                {},
                id="empty_meta",
            ),
            pytest.param(
                ["owner"],
                {"maturity": "high"},
                id="missing_key",
            ),
            pytest.param(
                ["owner", {"name": ["first", "last"]}],
                {"name": {"last": "Bobbington"}, "owner": "Bob"},
                id="missing_nested_key",
            ),
        ],
    )
    def test_missing_meta_keys(self, keys, meta):
        check_fails(
            "check_source_has_meta_keys",
            source={"meta": meta},
            keys=keys,
        )
