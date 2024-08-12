from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.manifest.check_sources import (
    check_source_description_populated,
    check_source_has_meta_keys,
    check_source_names,
    check_source_not_orphaned,
    check_source_property_file_location,
    check_source_used_by_models_in_same_directory,
    check_source_used_by_only_one_model,
)


@pytest.mark.parametrize(
    "source, expectation",
    [
        (
            {
                "description": "Description that is more than 4 characters.",
                "unique_id": "source.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "description": """A
                        multiline
                        description
                        """,
                "unique_id": "source.package_name.model_2",
            },
            does_not_raise(),
        ),
        (
            {
                "description": "",
                "unique_id": "source.package_name.model_3",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": " ",
                "unique_id": "source.package_name.model_4",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": """
                        """,
                "unique_id": "source.package_name.model_5",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": "-",
                "unique_id": "source.package_name.model_6",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": "null",
                "unique_id": "source.package_name.model_7",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_description_populated(source, expectation):
    with expectation:
        check_source_description_populated(source=source, request=None)


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
    "source, expectation",
    [
        (
            {
                "path": "models/staging/crm/_crm__sources.yml",
                "unique_id": "source.package_name.source_1.table_1",
            },
            does_not_raise(),
        ),
        (
            {
                "path": "models/staging/crm/_crm__source.yml",
                "unique_id": "source.package_name.source_1.table_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "path": "models/staging/crm/__source.yml",
                "unique_id": "source.package_name.source_1.table_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "path": "models/staging/crm/_staging__source.yml",
                "unique_id": "source.package_name.source_1.table_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "path": "models/staging/crm/crm__source.yml",
                "unique_id": "source.package_name.source_1.table_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_property_file_location(source, expectation):
    with expectation:
        check_source_property_file_location(source=source, request=None)


@pytest.mark.parametrize(
    "models, source, expectation",
    [
        (
            [
                {
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                    "path": "staging/model_1.sql",
                    "unique_id": "model.package_name.model_1",
                }
            ],
            {
                "path": "models/staging/_sources.yml",
                "unique_id": "source.package_name.source_1",
            },
            does_not_raise(),
        ),
        (
            [
                {
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                    "path": "staging/model_1.sql",
                    "unique_id": "model.package_name.model_1",
                }
            ],
            {
                "path": "models/_sources.yml",
                "unique_id": "source.package_name.source_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_used_by_models_in_same_directory(models, source, expectation):
    with expectation:
        check_source_used_by_models_in_same_directory(models=models, source=source, request=None)


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
