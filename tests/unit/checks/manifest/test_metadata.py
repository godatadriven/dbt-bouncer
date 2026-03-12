from dbt_bouncer.testing import check_fails, check_passes


def test_check_project_name_matches():
    check_passes(
        "check_project_name",
        project_name_pattern="^dbt_bouncer_",
    )


def test_check_project_name_does_not_match():
    check_fails(
        "check_project_name",
        project_name_pattern="^company_",
    )
