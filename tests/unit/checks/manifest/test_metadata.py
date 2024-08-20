from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.manifest.check_metadata import check_project_name


@pytest.mark.parametrize(
    "manifest_obj, project_name_pattern, expectation",
    [
        (
            "manifest_obj",
            "^dbt_bouncer_",
            does_not_raise(),
        ),
        (
            "manifest_obj",
            "^company_",
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_project_name(manifest_obj, project_name_pattern, expectation):
    with expectation:
        check_project_name(
            manifest_obj=manifest_obj,
            project_name_pattern=project_name_pattern,
            request=None,
        )
