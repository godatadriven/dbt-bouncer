from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.fast_parser import wrap_dict
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.sources.tags import (
    CheckSourceHasTags,
)


@pytest.mark.parametrize(
    ("source", "tags", "criteria", "expectation"),
    [
        (
            wrap_dict(
                {
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
                },
            ),
            ["tag_1"],
            "all",
            does_not_raise(),
        ),
        (
            wrap_dict(
                {
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
                },
            ),
            ["tag_1"],
            "all",
            pytest.raises(DbtBouncerFailedCheckError),
        ),
        (
            wrap_dict(
                {
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
                },
            ),
            ["tag_1", "tag_2"],
            "any",
            does_not_raise(),
        ),
        (
            wrap_dict(
                {
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
                    "tags": ["tag_3", "tag_4"],
                    "unique_id": "source.package_name.source_1.table_1",
                },
            ),
            ["tag_1", "tag_2"],
            "any",
            pytest.raises(DbtBouncerFailedCheckError),
        ),
        (
            wrap_dict(
                {
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
                    "tags": ["tag_1", "tag_3"],
                    "unique_id": "source.package_name.source_1.table_1",
                },
            ),
            ["tag_1", "tag_2"],
            "one",
            does_not_raise(),
        ),
        (
            wrap_dict(
                {
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
                    "tags": ["tag_1", "tag_2"],
                    "unique_id": "source.package_name.source_1.table_1",
                },
            ),
            ["tag_1", "tag_2"],
            "one",
            pytest.raises(DbtBouncerFailedCheckError),
        ),
        (
            wrap_dict(
                {
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
                },
            ),
            ["tag_1"],
            "one",
            pytest.raises(DbtBouncerFailedCheckError),
        ),
        (
            wrap_dict(
                {
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
                },
            ),
            ["tag_1"],
            "one",
            does_not_raise(),
        ),
    ],
)
def test_check_source_has_tags(source, tags, criteria, expectation):
    with expectation:
        CheckSourceHasTags(
            name="check_source_has_tags",
            source=source,
            tags=tags,
            criteria=criteria,
        ).execute()
