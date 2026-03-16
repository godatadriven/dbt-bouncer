from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceLoaderPopulated:
    def test_loader_populated(self):
        check_passes(
            "check_source_loader_populated",
            source={"loader": "Fivetran"},
        )

    def test_loader_empty(self):
        check_fails(
            "check_source_loader_populated",
            source={"loader": ""},
        )
