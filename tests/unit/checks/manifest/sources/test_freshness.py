import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceFreshnessPopulated:
    @pytest.mark.parametrize(
        "freshness",
        [
            pytest.param(
                {
                    "warn_after": {"count": 25, "period": "hour"},
                    "error_after": {"count": None, "period": None},
                    "filter": None,
                },
                id="warn_only",
            ),
            pytest.param(
                {
                    "warn_after": {"count": None, "period": None},
                    "error_after": {"count": 25, "period": "hour"},
                    "filter": None,
                },
                id="error_only",
            ),
            pytest.param(
                {
                    "warn_after": {"count": 25, "period": "hour"},
                    "error_after": {"count": 49, "period": "hour"},
                    "filter": None,
                },
                id="warn_and_error",
            ),
        ],
    )
    def test_freshness_populated(self, freshness):
        check_passes(
            "check_source_freshness_populated",
            source={"freshness": freshness},
        )

    @pytest.mark.parametrize(
        "freshness",
        [
            pytest.param(
                {
                    "warn_after": {"count": None, "period": None},
                    "error_after": {"count": None, "period": None},
                    "filter": None,
                },
                id="all_none",
            ),
            pytest.param(None, id="freshness_none"),
        ],
    )
    def test_freshness_not_populated(self, freshness):
        check_fails(
            "check_source_freshness_populated",
            source={"freshness": freshness},
        )
