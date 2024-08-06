from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.manifest.check_sources import (
    check_source_has_meta_keys,
    check_source_names,
    check_source_not_orphaned,
    check_source_used_by_only_one_model,
)


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


@pytest.mark.parametrize(
    "check_config, source, expectation",
    [
        (
            {
                "source_name_pattern": "^[a-z_]*$",
            },
            {
                "name": "model_a",
                "unique_id": "source.package_name.model_a",
            },
            does_not_raise(),
        ),
        (
            {
                "source_name_pattern": "^[a-z_]*$",
            },
            {
                "name": "model_1",
                "unique_id": "source.package_name.model_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_names(check_config, source, expectation):
    with expectation:
        check_source_names(check_config=check_config, source=source, request=None)


@pytest.mark.parametrize(
    "models, source, expectation",
    [
        (
            [
                {
                    "unique_id": "model.package_name.model_1",
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                }
            ],
            {
                "unique_id": "source.package_name.source_1",
            },
            does_not_raise(),
        ),
        (
            [
                {
                    "unique_id": "model.package_name.model_1",
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                },
                {
                    "unique_id": "model.package_name.model_2",
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                },
            ],
            {
                "unique_id": "source.package_name.source_1",
            },
            does_not_raise(),
        ),
        (
            [
                {
                    "unique_id": "model.package_name.model_1",
                    "depends_on": {"nodes": []},
                },
            ],
            {
                "unique_id": "source.package_name.source_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_not_orphaned(models, source, expectation):
    with expectation:
        check_source_not_orphaned(models=models, source=source, request=None)


@pytest.mark.parametrize(
    "models, source, expectation",
    [
        (
            [
                {
                    "unique_id": "model.package_name.model_1",
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                }
            ],
            {
                "unique_id": "source.package_name.source_1",
            },
            does_not_raise(),
        ),
        (
            [
                {
                    "unique_id": "model.package_name.model_1",
                    "depends_on": {"nodes": []},
                }
            ],
            {
                "unique_id": "source.package_name.source_1",
            },
            does_not_raise(),
        ),
        (
            [
                {
                    "unique_id": "model.package_name.model_1",
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                },
                {
                    "unique_id": "model.package_name.model_2",
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                },
            ],
            {
                "unique_id": "source.package_name.source_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_used_by_only_one_model(models, source, expectation):
    with expectation:
        check_source_used_by_only_one_model(models=models, source=source, request=None)
