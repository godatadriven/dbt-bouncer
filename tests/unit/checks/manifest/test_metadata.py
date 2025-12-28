from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.parsers_manifest import (
    DbtBouncerManifest,  # noqa: F401
)
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.check_metadata import CheckProjectName

CheckProjectName.model_rebuild()


@pytest.mark.parametrize(
    ("manifest_obj", "project_name_pattern", "expectation"),
    [
        (
            "manifest_obj",
            "^dbt_bouncer_",
            does_not_raise(),
        ),
        (
            "manifest_obj",
            "^company_",
            pytest.raises(DbtBouncerFailedCheckError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_project_name(manifest_obj, project_name_pattern, expectation):
    with expectation:
        CheckProjectName(
            manifest_obj=manifest_obj,
            name="check_project_name",
            project_name_pattern=project_name_pattern,
        ).execute()
