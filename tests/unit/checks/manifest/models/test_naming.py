import re

import pytest

from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError
from dbt_bouncer.testing import _run_check, check_fails, check_passes


class TestCheckModelNames:
    @pytest.mark.parametrize(
        ("include", "model_name_pattern", "model"),
        [
            pytest.param(
                "",
                "^stg_",
                {
                    "alias": "stg_model_1",
                    "fqn": ["package_name", "stg_model_1"],
                    "name": "stg_model_1",
                    "original_file_path": "models/staging/stg_model_1.sql",
                    "path": "staging/stg_model_1.sql",
                    "unique_id": "model.package_name.stg_model_1",
                },
                id="valid_name_stg",
            ),
            pytest.param(
                "^staging",
                "^stg_",
                {
                    "alias": "stg_model_2",
                    "fqn": ["package_name", "stg_model_2"],
                    "name": "stg_model_2",
                    "original_file_path": "models/staging/stg_model_2.sql",
                    "path": "staging/stg_model_2.sql",
                    "unique_id": "model.package_name.stg_model_2",
                },
                id="valid_name_staging_dir",
            ),
            pytest.param(
                "^intermediate",
                "^stg_",
                {
                    "alias": "stg_model_3",
                    "fqn": ["package_name", "stg_model_3"],
                    "name": "stg_model_3",
                    "original_file_path": "models/staging/stg_model_3.sql",
                    "path": "staging/stg_model_3.sql",
                    "unique_id": "model.package_name.stg_model_3",
                },
                id="valid_name_ignored_dir",
            ),
            pytest.param(
                "^intermediate",
                "^int_",
                {
                    "alias": "int_model_1",
                    "fqn": ["package_name", "int_model_1"],
                    "name": "int_model_1",
                    "original_file_path": "models/intermediate/int_model_1.sql",
                    "path": "intermediate/int_model_1.sql",
                    "unique_id": "model.package_name.int_model_1",
                },
                id="valid_name_int",
            ),
            pytest.param(
                "",
                ".*_v1$",
                {
                    "alias": "model_v1",
                    "fqn": ["package_name", "model_v1"],
                    "name": "model_v1",
                    "original_file_path": "models/staging/model_v1.sql",
                    "path": "staging/model_v1.sql",
                    "unique_id": "model.package_name.model_v1",
                },
                id="suffix_with_wildcard_prefix",
            ),
            pytest.param(
                "",
                "stg_",
                {
                    "alias": "stg_orders",
                    "fqn": ["package_name", "stg_orders"],
                    "name": "stg_orders",
                    "original_file_path": "models/staging/stg_orders.sql",
                    "path": "staging/stg_orders.sql",
                    "unique_id": "model.package_name.stg_orders",
                },
                id="no_anchor_start_match",
            ),
            pytest.param(
                "",
                "^stg_orders",
                {
                    "alias": "stg_orders_backup",
                    "fqn": ["package_name", "stg_orders_backup"],
                    "name": "stg_orders_backup",
                    "original_file_path": "models/staging/stg_orders_backup.sql",
                    "path": "staging/stg_orders_backup.sql",
                    "unique_id": "model.package_name.stg_orders_backup",
                },
                id="no_implicit_end_anchor",
            ),
            pytest.param(
                "",
                "^(stg|int|fct|dim)_",
                {
                    "alias": "fct_orders",
                    "fqn": ["package_name", "fct_orders"],
                    "name": "fct_orders",
                    "original_file_path": "models/marts/fct_orders.sql",
                    "path": "marts/fct_orders.sql",
                    "unique_id": "model.package_name.fct_orders",
                },
                id="alternation_match",
            ),
            pytest.param(
                "",
                "",
                {
                    "alias": "anything",
                    "fqn": ["package_name", "anything"],
                    "name": "anything",
                    "original_file_path": "models/staging/anything.sql",
                    "path": "staging/anything.sql",
                    "unique_id": "model.package_name.anything",
                },
                id="empty_pattern_matches_everything",
            ),
            pytest.param(
                "",
                "   ",
                {
                    "alias": "anything",
                    "fqn": ["package_name", "anything"],
                    "name": "anything",
                    "original_file_path": "models/staging/anything.sql",
                    "path": "staging/anything.sql",
                    "unique_id": "model.package_name.anything",
                },
                id="whitespace_only_pattern_matches_everything",
            ),
            pytest.param(
                "",
                "  ^stg_  ",
                {
                    "alias": "stg_orders",
                    "fqn": ["package_name", "stg_orders"],
                    "name": "stg_orders",
                    "original_file_path": "models/staging/stg_orders.sql",
                    "path": "staging/stg_orders.sql",
                    "unique_id": "model.package_name.stg_orders",
                },
                id="padded_pattern_passes_like_stripped",
            ),
            pytest.param(
                "",
                "^dim_",
                {
                    "alias": "dim_customers",
                    "fqn": ["package_name", "dim_customers", "v1"],
                    "name": "dim_customers",
                    "original_file_path": "models/marts/dim_customers.sql",
                    "path": "marts/dim_customers.sql",
                    "unique_id": "model.package_name.dim_customers.v1",
                },
                id="versioned_name_v1",
            ),
            pytest.param(
                "",
                "^dim_",
                {
                    "alias": "dim_customers",
                    "fqn": ["package_name", "dim_customers", "v2"],
                    "name": "dim_customers",
                    "original_file_path": "models/marts/dim_customers.sql",
                    "path": "marts/dim_customers.sql",
                    "unique_id": "model.package_name.dim_customers.v2",
                },
                id="versioned_name_v2",
            ),
        ],
    )
    def test_passes(self, include, model_name_pattern, model):
        check_passes(
            "check_model_names",
            include=include,
            model_name_pattern=model_name_pattern,
            model=model,
        )

    @pytest.mark.parametrize(
        ("include", "model_name_pattern", "model"),
        [
            pytest.param(
                "^intermediate",
                "^int_",
                {
                    "alias": "model_1",
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "models/intermediate/model_1.sql",
                    "path": "intermediate/model_1.sql",
                    "unique_id": "model.package_name.model_1",
                },
                id="invalid_name_int",
            ),
            pytest.param(
                "^intermediate",
                "^int_",
                {
                    "alias": "model_int_2",
                    "fqn": ["package_name", "model_int_2"],
                    "name": "model_int_2",
                    "original_file_path": "models/intermediate/model_int_2.sql",
                    "path": "intermediate/model_int_2.sql",
                    "unique_id": "model.package_name.model_int_2",
                },
                id="invalid_name_int_suffix",
            ),
            pytest.param(
                "",
                "_v1$",
                {
                    "alias": "model_v1",
                    "fqn": ["package_name", "model_v1"],
                    "name": "model_v1",
                    "original_file_path": "models/staging/model_v1.sql",
                    "path": "staging/model_v1.sql",
                    "unique_id": "model.package_name.model_v1",
                },
                id="suffix_silently_never_matches",
            ),
            pytest.param(
                "",
                "stg_",
                {
                    "alias": "orders_stg_daily",
                    "fqn": ["package_name", "orders_stg_daily"],
                    "name": "orders_stg_daily",
                    "original_file_path": "models/staging/orders_stg_daily.sql",
                    "path": "staging/orders_stg_daily.sql",
                    "unique_id": "model.package_name.orders_stg_daily",
                },
                id="no_anchor_not_matched_mid_string",
            ),
            pytest.param(
                "",
                "^stg_orders$",
                {
                    "alias": "stg_orders_backup",
                    "fqn": ["package_name", "stg_orders_backup"],
                    "name": "stg_orders_backup",
                    "original_file_path": "models/staging/stg_orders_backup.sql",
                    "path": "staging/stg_orders_backup.sql",
                    "unique_id": "model.package_name.stg_orders_backup",
                },
                id="exact_name_needs_end_anchor",
            ),
            pytest.param(
                "",
                "^stg_",
                {
                    "alias": "STG_orders",
                    "fqn": ["package_name", "STG_orders"],
                    "name": "STG_orders",
                    "original_file_path": "models/staging/STG_orders.sql",
                    "path": "staging/STG_orders.sql",
                    "unique_id": "model.package_name.STG_orders",
                },
                id="case_sensitive_no_ignorecase",
            ),
            pytest.param(
                "",
                "^(stg|int|fct|dim)_",
                {
                    "alias": "mart_orders",
                    "fqn": ["package_name", "mart_orders"],
                    "name": "mart_orders",
                    "original_file_path": "models/marts/mart_orders.sql",
                    "path": "marts/mart_orders.sql",
                    "unique_id": "model.package_name.mart_orders",
                },
                id="alternation_no_match",
            ),
        ],
    )
    def test_fails(self, include, model_name_pattern, model):
        check_fails(
            "check_model_names",
            include=include,
            model_name_pattern=model_name_pattern,
            model=model,
        )

    def test_pattern_whitespace_stripped_in_failure_message(self):
        # "  ^stg_  " → message shows the STRIPPED pattern in backticks, no padding.
        with pytest.raises(DbtBouncerFailedCheckError, match=re.escape("`^stg_`")):
            _run_check(
                "check_model_names",
                model_name_pattern="  ^stg_  ",
                model={
                    "name": "orders",
                    "unique_id": "model.package_name.orders",
                    "path": "staging/orders.sql",
                    "original_file_path": "models/staging/orders.sql",
                },
            )

    def test_invalid_regex_pattern_raises(self):
        # compile_pattern re-raises re.error with the "Invalid regex pattern" prefix.
        with pytest.raises(re.error, match="Invalid regex pattern"):
            _run_check(
                "check_model_names",
                model_name_pattern="^stg_(",
                model={
                    "name": "stg_orders",
                    "unique_id": "model.package_name.stg_orders",
                    "path": "staging/stg_orders.sql",
                    "original_file_path": "models/staging/stg_orders.sql",
                },
            )

    def test_versioned_model_display_name_in_failure_message(self):
        # name is "dim_customers"; unique_id carries the version → display "dim_customers_v2".
        with pytest.raises(DbtBouncerFailedCheckError, match="dim_customers_v2"):
            _run_check(
                "check_model_names",
                model_name_pattern="^stg_",
                model={
                    "name": "dim_customers",
                    "unique_id": "model.package_name.dim_customers.v2",
                    "path": "marts/dim_customers.sql",
                    "original_file_path": "models/marts/dim_customers.sql",
                },
            )
