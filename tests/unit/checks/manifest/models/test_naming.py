import re

import pytest

from dbt_bouncer.testing import check_fails, check_passes


def _model(name: str, *, version: str | None = None) -> dict:
    """Build a model override dict.

    ``check_model_names`` only reads ``name`` and ``unique_id``, so everything
    else is left to the default model in ``dbt_bouncer.testing``.

    Returns:
        dict: A manifest model node dict.

    """
    unique_id = f"model.package_name.{name}"
    if version is not None:
        unique_id = f"{unique_id}.{version}"
    return {"name": name, "unique_id": unique_id}


class TestCheckModelNames:
    @pytest.mark.parametrize(
        ("model_name_pattern", "model", "check_fn"),
        [
            pytest.param(
                "^stg_", _model("stg_model_1"), check_passes, id="valid_name_stg"
            ),
            pytest.param(
                "^int_", _model("int_model_1"), check_passes, id="valid_name_int"
            ),
            pytest.param(
                ".*_v1$",
                _model("model_v1"),
                check_passes,
                id="suffix_with_wildcard_prefix",
            ),
            pytest.param(
                "stg_", _model("stg_orders"), check_passes, id="no_anchor_start_match"
            ),
            pytest.param(
                "^stg_orders",
                _model("stg_orders_backup"),
                check_passes,
                id="no_implicit_end_anchor",
            ),
            pytest.param(
                "^(stg|int|fct|dim)_",
                _model("fct_orders"),
                check_passes,
                id="alternation_match",
            ),
            pytest.param(
                "",
                _model("anything"),
                check_passes,
                id="empty_pattern_matches_everything",
            ),
            pytest.param(
                "   ",
                _model("anything"),
                check_passes,
                id="whitespace_only_pattern_matches_everything",
            ),
            pytest.param(
                "  ^stg_  ",
                _model("stg_orders"),
                check_passes,
                id="padded_pattern_passes_like_stripped",
            ),
            pytest.param(
                "(?i)^STG_",
                _model("stg_orders"),
                check_passes,
                id="inline_ignorecase_flag_honoured",
            ),
            pytest.param(
                "^dim_",
                _model("dim_customers", version="v1"),
                check_passes,
                id="versioned_name_v1",
            ),
            pytest.param(
                "^dim_",
                _model("dim_customers", version="v2"),
                check_passes,
                id="versioned_name_v2",
            ),
            pytest.param(
                "^int_", _model("model_1"), check_fails, id="invalid_name_int"
            ),
            pytest.param(
                "^int_",
                _model("model_int_2"),
                check_fails,
                id="invalid_name_int_suffix",
            ),
            pytest.param(
                "_v1$",
                _model("model_v1"),
                check_fails,
                id="suffix_without_wildcard_prefix_fails",
            ),
            pytest.param(
                "stg_",
                _model("orders_stg_daily"),
                check_fails,
                id="no_anchor_not_matched_mid_string",
            ),
            pytest.param(
                "^stg_orders$",
                _model("stg_orders_backup"),
                check_fails,
                id="exact_name_needs_end_anchor",
            ),
            pytest.param(
                "^stg_",
                _model("STG_orders"),
                check_fails,
                id="case_sensitive_no_ignorecase",
            ),
            pytest.param(
                "^(stg|int|fct|dim)_",
                _model("mart_orders"),
                check_fails,
                id="alternation_no_match",
            ),
        ],
    )
    def test_check_model_names(self, model_name_pattern, model, check_fn):
        check_fn(
            "check_model_names",
            model_name_pattern=model_name_pattern,
            model=model,
        )

    def test_pattern_whitespace_stripped_in_failure_message(self):
        # "  ^stg_  " → message shows the STRIPPED pattern in backticks, no padding.
        check_fails(
            "check_model_names",
            model_name_pattern="  ^stg_  ",
            model=_model("orders"),
            match=re.escape("`^stg_`"),
        )

    def test_invalid_regex_pattern_raises(self):
        # compile_pattern re-raises re.error with the "Invalid regex pattern" prefix.
        check_fails(
            "check_model_names",
            model_name_pattern="^stg_(",
            model=_model("stg_orders"),
            expected_exception=re.error,
            match="Invalid regex pattern",
        )

    def test_versioned_model_display_name_in_failure_message(self):
        # name is "dim_customers"; unique_id carries the version → display "dim_customers_v2".
        check_fails(
            "check_model_names",
            model_name_pattern="^stg_",
            model=_model("dim_customers", version="v2"),
            match="dim_customers_v2",
        )
