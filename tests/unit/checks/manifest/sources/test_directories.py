from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.parser import wrap_dict
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.sources.directories import (
    CheckSourcePropertyFileLocation,
)


@pytest.mark.parametrize(
    ("source", "expectation"),
    [
        (
            wrap_dict(
                {
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
                },
            ),
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
                    "original_file_path": "models/staging/crm/_crm__source.yml",
                    "package_name": "package_name",
                    "path": "models/staging/crm/_crm__source.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                },
            ),
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
                    "original_file_path": "models/staging/crm/__source.yml",
                    "package_name": "package_name",
                    "path": "models/staging/crm/__source.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                },
            ),
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
                    "original_file_path": "models/staging/crm/_staging__source.yml",
                    "package_name": "package_name",
                    "path": "models/staging/crm/_staging__source.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                },
            ),
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
                    "original_file_path": "models/staging/crm/crm__source.yml",
                    "package_name": "package_name",
                    "path": "models/staging/crm/crm__source.yml",
                    "resource_type": "source",
                    "schema": "main",
                    "source_description": "",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.table_1",
                },
            ),
            pytest.raises(DbtBouncerFailedCheckError),
        ),
    ],
)
def test_check_source_property_file_location(source, expectation):
    with expectation:
        CheckSourcePropertyFileLocation(
            name="check_source_property_file_location",
            source=source,
        ).execute()
