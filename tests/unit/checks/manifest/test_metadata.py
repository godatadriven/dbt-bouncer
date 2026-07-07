import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckProjectName:
    @pytest.mark.parametrize(
        ("project_name_pattern", "check_fn"),
        [
            pytest.param(
                "^dbt_bouncer_",
                check_passes,
                id="project_name_matches_pattern",
            ),
            pytest.param(
                "^company_",
                check_fails,
                id="project_name_does_not_match_pattern",
            ),
        ],
    )
    def test_check_project_name(self, project_name_pattern, check_fn):
        check_fn(
            "check_project_name",
            project_name_pattern=project_name_pattern,
        )
