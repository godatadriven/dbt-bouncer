import pytest
from pydantic import ValidationError

from dbt_bouncer.testing import _run_check, check_fails, check_passes


class TestCheckRunResultsMaxGigabytesBilled:
    @pytest.mark.parametrize(
        ("run_result", "check_fn"),
        [
            pytest.param({}, check_passes, id="within_limit"),
            pytest.param(
                {"adapter_response": {"bytes_billed": 100000000000}},
                check_fails,
                id="exceeds_limit",
            ),
        ],
    )
    def test_check_run_results_max_gigabytes_billed(self, run_result, check_fn):
        check_fn(
            "check_run_results_max_gigabytes_billed",
            run_result=run_result,
            max_gigabytes_billed=10,
        )

    @pytest.mark.parametrize(
        "run_result",
        [
            # dbt-bigquery writes an empty adapter response for nodes that ran no
            # query. This used to raise "`bytes_billed` not found" on every one.
            pytest.param(
                {"adapter_response": {}, "status": "skipped"}, id="skipped_node"
            ),
            pytest.param(
                {"adapter_response": {}, "status": "error"}, id="errored_node"
            ),
        ],
    )
    def test_node_that_ran_no_query_passes(self, run_result):
        check_passes(
            "check_run_results_max_gigabytes_billed",
            run_result=run_result,
            max_gigabytes_billed=10,
        )

    def test_non_bigquery_adapter_response_still_raises(self):
        # A populated response without `bytes_billed` means a different adapter,
        # which the check cannot evaluate.
        check_fails(
            "check_run_results_max_gigabytes_billed",
            run_result={"adapter_response": {"_message": "OK", "rows_affected": 1}},
            max_gigabytes_billed=10,
            expected_exception=RuntimeError,
            match="`bytes_billed` not found",
        )


class TestCheckRunResultsMaxExecutionTime:
    @pytest.mark.parametrize(
        ("run_result", "check_fn"),
        [
            pytest.param({}, check_passes, id="within_limit"),
            pytest.param({"execution_time": 10}, check_passes, id="at_limit"),
            pytest.param({"execution_time": 100}, check_fails, id="exceeds_limit"),
        ],
    )
    def test_check_run_results_max_execution_time(self, run_result, check_fn):
        check_fn(
            "check_run_results_max_execution_time",
            run_result=run_result,
            max_execution_time_seconds=10,
        )

    @pytest.mark.parametrize(
        "max_execution_time_seconds",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_invalid_param_rejected_at_config_load(self, max_execution_time_seconds):
        with pytest.raises(ValidationError, match="greater than 0"):
            _run_check(
                "check_run_results_max_execution_time",
                run_result={},
                max_execution_time_seconds=max_execution_time_seconds,
            )


class TestCheckRunResultsMaxGigabytesBilledInvalidParam:
    @pytest.mark.parametrize(
        "max_gigabytes_billed",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_rejected_at_config_load(self, max_gigabytes_billed):
        with pytest.raises(ValidationError, match="greater than 0"):
            _run_check(
                "check_run_results_max_gigabytes_billed",
                run_result={},
                max_gigabytes_billed=max_gigabytes_billed,
            )
