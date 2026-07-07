import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceLoaderPopulated:
    @pytest.mark.parametrize(
        ("source", "check_fn"),
        [
            pytest.param({"loader": "Fivetran"}, check_passes, id="loader_populated"),
            pytest.param({"loader": ""}, check_fails, id="loader_empty"),
        ],
    )
    def test_check_source_loader_populated(self, source, check_fn):
        check_fn(
            "check_source_loader_populated",
            source=source,
        )
