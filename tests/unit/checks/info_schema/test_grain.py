import pytest

from dbt_bouncer.artifact_parsers.info_schema import InfoSchema
from dbt_bouncer.testing import check_fails, check_passes

MODEL = "model.package_name.model_1"


def _info_schema(grain, grain_tested):
    return InfoSchema.from_rows(
        models=[{"unique_id": MODEL, "grain": grain, "grain_tested": grain_tested}]
    )


class TestCheckModelGrainIsTested:
    @pytest.mark.parametrize(
        ("grain", "grain_tested", "check_fn"),
        [
            pytest.param(["id"], ["id"], check_passes, id="tested"),
            pytest.param(["ID"], ["id"], check_passes, id="case_insensitive"),
            pytest.param([], [], check_passes, id="no_grain"),
            pytest.param(["id"], [], check_fails, id="untested"),
            pytest.param(
                ["order_id", "line"], ["order_id"], check_fails, id="partially_tested"
            ),
        ],
    )
    def test_grain_is_tested(self, grain, grain_tested, check_fn):
        check_fn(
            "check_model_grain_is_tested",
            model={"unique_id": MODEL},
            ctx_info_schema=_info_schema(grain, grain_tested),
        )

    def test_model_missing_from_info_schema_passes(self):
        check_passes(
            "check_model_grain_is_tested",
            model={"unique_id": MODEL},
            ctx_info_schema=InfoSchema.from_rows(models=[]),
        )


class TestCheckModelHasGrain:
    @pytest.mark.parametrize(
        ("grain", "check_fn"),
        [
            pytest.param(["id"], check_passes, id="has_grain"),
            pytest.param([], check_fails, id="empty_grain"),
            pytest.param(None, check_fails, id="null_grain"),
        ],
    )
    def test_has_grain(self, grain, check_fn):
        check_fn(
            "check_model_has_grain",
            model={"unique_id": MODEL},
            ctx_info_schema=_info_schema(grain, []),
        )

    def test_model_missing_from_info_schema_passes(self):
        check_passes(
            "check_model_has_grain",
            model={"unique_id": MODEL},
            ctx_info_schema=InfoSchema.from_rows(models=[]),
        )
