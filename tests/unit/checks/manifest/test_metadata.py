from contextlib import nullcontext as does_not_raise

import pytest

from src.dbt_bouncer.checks.manifest.check_metadata import check_project_name


@pytest.mark.parametrize(
    "check_config, manifest_obj, expectation",
    [
        (
            {
                "project_name_pattern": "^dbt_bouncer_",
            },
            "manifest_obj",
            does_not_raise(),
        ),
        (
            {
                "project_name_pattern": "^company_",
            },
            "manifest_obj",
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_project_name(check_config, manifest_obj, expectation, request):
    with expectation:
        check_project_name(
            check_config=check_config,
            manifest_obj=manifest_obj,
            request=None,
        )
