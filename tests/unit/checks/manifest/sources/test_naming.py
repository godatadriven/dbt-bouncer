from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.parser import wrap_dict
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.sources.naming import (
    CheckSourceNames,
)


@pytest.mark.parametrize(
    ("source_name_pattern", "source", "expectation"),
    [
        (
            "^[a-z_]*$",
            wrap_dict(
                {
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
                },
            ),
            does_not_raise(),
        ),
        (
            "^[a-z_]*$",
            wrap_dict(
                {
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
                },
            ),
            pytest.raises(DbtBouncerFailedCheckError),
        ),
    ],
)
def test_check_source_names(source_name_pattern, source, expectation):
    with expectation:
        CheckSourceNames(
            name="check_source_names",
            source_name_pattern=source_name_pattern,
            source=source,
        ).execute()
