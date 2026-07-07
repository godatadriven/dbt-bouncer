import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceHasLabelsKeys:
    @pytest.mark.parametrize(
        ("keys", "source"),
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
    def test_passes(self, keys, source):
        check_passes("check_source_has_labels_keys", keys=keys, source=source)

    @pytest.mark.parametrize(
        ("keys", "source"),
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
                [{"team": ["subteam"]}],
                {"config": {"labels": {"team": {"other": "value"}}}},
                id="missing_nested_key",
            ),
        ],
    )
    def test_fails(self, keys, source):
        check_fails("check_source_has_labels_keys", keys=keys, source=source)


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


class TestCheckSourcePiiMeta:
    @pytest.mark.parametrize(
        ("source_override", "column_name_pattern", "meta_key"),
        [
            pytest.param(
                {
                    "columns": {
                        "email_address": {
                            "name": "email_address",
                            "meta": {"pii": True},
                        },
                    },
                },
                "^email_.*",
                "pii",
                id="matching_column_has_meta_key",
            ),
            pytest.param(
                {
                    "columns": {
                        "order_id": {
                            "name": "order_id",
                            "meta": {},
                        },
                    },
                },
                "^email_.*",
                "pii",
                id="no_matching_columns",
            ),
            pytest.param(
                {"columns": {}},
                "^email_.*",
                "pii",
                id="empty_columns",
            ),
            pytest.param(
                {"columns": None},
                "^email_.*",
                "pii",
                id="none_columns",
            ),
        ],
    )
    def test_pass(self, source_override, column_name_pattern, meta_key):
        check_passes(
            "check_source_pii_meta",
            source=source_override,
            column_name_pattern=column_name_pattern,
            meta_key=meta_key,
        )

    @pytest.mark.parametrize(
        ("source_override", "column_name_pattern", "meta_key"),
        [
            pytest.param(
                {
                    "columns": {
                        "email_address": {
                            "name": "email_address",
                            "meta": {},
                        },
                    },
                },
                "^email_.*",
                "pii",
                id="matching_column_missing_meta_key",
            ),
            pytest.param(
                {
                    "columns": {
                        "email_address": {
                            "name": "email_address",
                            "meta": {"owner": "data-team"},
                        },
                    },
                },
                "^email_.*",
                "pii",
                id="matching_column_has_different_meta_key",
            ),
        ],
    )
    def test_fail(self, source_override, column_name_pattern, meta_key):
        check_fails(
            "check_source_pii_meta",
            source=source_override,
            column_name_pattern=column_name_pattern,
            meta_key=meta_key,
        )
