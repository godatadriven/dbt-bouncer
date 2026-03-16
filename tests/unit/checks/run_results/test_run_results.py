from dbt_bouncer.testing import check_fails, check_passes


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
