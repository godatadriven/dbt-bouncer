from contextlib import nullcontext as does_not_raise

import pytest
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes4, Sources

from dbt_bouncer.checks.manifest.check_sources import (
    check_source_description_populated,
    check_source_freshness_populated,
    check_source_has_meta_keys,
    check_source_has_tags,
    check_source_loader_populated,
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
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Sources(
                **{
                    "description": """A
                            multiline
                            description
                            """,
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Sources(
                **{
                    "description": " ",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Sources(
                **{
                    "description": """
                            """,
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Sources(
                **{
                    "description": "-",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Sources(
                **{
                    "description": "null",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_description_populated(source, expectation):
    with expectation:
        check_source_description_populated(source=source, request=None)


@pytest.mark.parametrize(
    "source, expectation",
    [
        (
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "freshness": {
                        "warn_after": {"count": 25, "period": "hour"},
                        "error_after": {"count": None, "period": None},
                        "filter": None,
                    },
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "freshness": {
                        "warn_after": {"count": None, "period": None},
                        "error_after": {"count": 25, "period": "hour"},
                        "filter": None,
                    },
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "freshness": {
                        "warn_after": {"count": 25, "period": "hour"},
                        "error_after": {"count": 49, "period": "hour"},
                        "filter": None,
                    },
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "freshness": {
                        "warn_after": {"count": None, "period": None},
                        "error_after": {"count": None, "period": None},
                        "filter": None,
                    },
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_freshness_populated(source, expectation):
    with expectation:
        check_source_freshness_populated(source=source, request=None)


@pytest.mark.parametrize(
    "source, expectation",
    [
        (
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "Fivetran",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_loader_populated(source, expectation):
    with expectation:
        check_source_loader_populated(source=source, request=None)


@pytest.mark.parametrize(
    "check_config, source, expectation",
    [
        (
            {"keys": ["owner"]},
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "meta": {"owner": "Bob"},
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            {"keys": ["owner"]},
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "meta": {"maturity": "high", "owner": "Bob"},
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            {"keys": ["owner", {"name": ["first", "last"]}]},
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "meta": {"name": {"first": "Bob", "last": "Bobbington"}, "owner": "Bob"},
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            {"keys": ["owner"]},
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "meta": {},
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            {"keys": ["owner"]},
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "meta": {"maturity": "high"},
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            {"keys": ["owner", {"name": ["first", "last"]}]},
            Sources(
                **{
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "meta": {"name": {"last": "Bobbington"}, "owner": "Bob"},
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
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
                "tags": ["tag_1"],
            },
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            {
                "tags": ["tag_1"],
            },
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": [],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_has_tags(check_config, source, expectation):
    with expectation:
        check_source_has_tags(check_config=check_config, source=source, request=None)


@pytest.mark.parametrize(
    "check_config, source, expectation",
    [
        (
            {
                "source_name_pattern": "^[a-z_]*$",
            },
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "model_a"],
                    "identifier": "model_a",
                    "loader": "",
                    "name": "model_a",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.model_a",
                }
            ),
            does_not_raise(),
        ),
        (
            {
                "source_name_pattern": "^[a-z_]*$",
            },
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "model_1"],
                    "identifier": "model_1",
                    "loader": "",
                    "name": "model_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.model_1",
                }
            ),
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
                Nodes4(
                    **{
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    }
                )
            ],
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            [
                Nodes4(
                    **{
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    }
                ),
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "model_2.sql",
                        "package_name": "package_name",
                        "path": "model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                ),
            ],
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            [
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {"nodes": []},
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "model_2.sql",
                        "package_name": "package_name",
                        "path": "model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                )
            ],
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
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
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/crm/_crm__sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/crm/_crm__sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/crm/_crm__source.yml",
                    "package_name": "package_name",
                    "path": "models/staging/crm/_crm__source.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/crm/__source.yml",
                    "package_name": "package_name",
                    "path": "models/staging/crm/__source.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/crm/_staging__source.yml",
                    "package_name": "package_name",
                    "path": "models/staging/crm/_staging__source.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/crm/crm__source.yml",
                    "package_name": "package_name",
                    "path": "models/staging/crm/crm__source.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
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
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "models/staging/model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                )
            ],
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/staging/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/staging/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            [
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "models/staging/model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                )
            ],
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
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
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "models/staging/model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                )
            ],
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            [
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {"nodes": []},
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "models/staging/model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                )
            ],
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            does_not_raise(),
        ),
        (
            [
                Nodes4(
                    **{
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "models/staging/model_1.sql",
                        "package_name": "package_name",
                        "path": "staging/model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    }
                ),
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "models/staging/model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                ),
            ],
            Sources(
                **{
                    "description": "",
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "",
                    "name": "table_1",
                    "original_file_path": "models/_sources.yml",
                    "package_name": "package_name",
                    "path": "models/_sources.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_source_used_by_only_one_model(models, source, expectation):
    with expectation:
        check_source_used_by_only_one_model(models=models, source=source, request=None)
