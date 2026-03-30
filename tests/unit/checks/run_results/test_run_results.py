import pytest

from dbt_bouncer.testing import _run_check, check_fails, check_passes


class TestCheckRunResultsMaxGigabytesBilled:
    def test_within_limit(self):
        check_passes(
            "check_run_results_max_gigabytes_billed",
            run_result={},
            max_gigabytes_billed=10,
        )

    def test_exceeds_limit(self):
        check_fails(
            "check_run_results_max_gigabytes_billed",
            run_result={"adapter_response": {"bytes_billed": 100000000000}},
            max_gigabytes_billed=10,
        )


class TestCheckRunResultsMaxExecutionTime:
    def test_within_limit(self):
        check_passes(
            "check_run_results_max_execution_time",
            run_result={},
            max_execution_time_seconds=10,
        )

    def test_at_limit(self):
        check_passes(
            "check_run_results_max_execution_time",
            run_result={"execution_time": 10},
            max_execution_time_seconds=10,
        )

    def test_exceeds_limit(self):
        check_fails(
            "check_run_results_max_execution_time",
            run_result={"execution_time": 100},
            max_execution_time_seconds=10,
        )

    @pytest.mark.parametrize(
        "max_execution_time_seconds",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error_for_invalid_param(self, max_execution_time_seconds):
        with pytest.raises(ValueError, match="must be positive"):
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
    def test_raises_value_error(self, max_gigabytes_billed):
        with pytest.raises(ValueError, match="must be positive"):
            _run_check(
                "check_run_results_max_gigabytes_billed",
                run_result={},
                max_gigabytes_billed=max_gigabytes_billed,
            )
