from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.check_sources import check_source_has_meta_keys


@pytest.mark.parametrize(
    "check_config, source, expectation",
    [
        (
            {"keys": ["owner"]},
            {
                "meta": {"owner": "Bob"},
                "unique_id": "source.package_name.source_1",
            },
            does_not_raise(),
        ),
        (
            {"keys": ["owner"]},
            {
                "meta": {"maturity": "high", "owner": "Bob"},
                "unique_id": "source.package_name.source_1",
            },
            does_not_raise(),
        ),
        (
            {"keys": ["owner", {"name": ["first", "last"]}]},
            {
                "meta": {"name": {"first": "Bob", "last": "Bobbington"}, "owner": "Bob"},
                "unique_id": "source.package_name.source_1",
            },
            does_not_raise(),
        ),
        (
            {"keys": ["owner"]},
            {
                "meta": {},
                "unique_id": "source.package_name.source_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {"keys": ["owner"]},
            {
                "meta": {"maturity": "high"},
                "unique_id": "source.package_name.source_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {"keys": ["owner", {"name": ["first", "last"]}]},
            {
                "meta": {"name": {"last": "Bobbington"}, "owner": "Bob"},
                "unique_id": "source.package_name.source_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_has_meta_keys(check_config, source, expectation):
    with expectation:
        check_source_has_meta_keys(check_config=check_config, source=source, request=None)
