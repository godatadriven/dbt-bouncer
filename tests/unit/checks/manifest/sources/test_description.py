from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import Sources
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.sources.description import (
    CheckSourceDescriptionPopulated,
)


@pytest.mark.parametrize(
    ("source", "expectation"),
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
                },
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
                },
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
                },
            ),
            pytest.raises(DbtBouncerFailedCheckError),
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
                },
            ),
            pytest.raises(DbtBouncerFailedCheckError),
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
                },
            ),
            pytest.raises(DbtBouncerFailedCheckError),
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
                },
            ),
            pytest.raises(DbtBouncerFailedCheckError),
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
                },
            ),
            pytest.raises(DbtBouncerFailedCheckError),
        ),
    ],
)
def test_check_source_description_populated(source, expectation):
    with expectation:
        CheckSourceDescriptionPopulated(
            name="check_source_description_populated",
            source=source,
        ).execute()
