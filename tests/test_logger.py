import logging

from click.testing import CliRunner

from dbt_bouncer.logger import logger
from dbt_bouncer.main import cli
from tests.pytest_helpers import catch_logs, records_to_tuples


def test_logger_debug() -> None:
    with catch_logs(level=logging.DEBUG, logger=logger) as handler:
        runner = CliRunner()
        runner.invoke(
            cli,
            ["--dbt-project-dir", "dbt_project"],
        )

        assert (
            len(
                [
                    record
                    for record in records_to_tuples(handler.records)
                    if record[2].startswith("dbt_project_dir=")
                ]
            )
            == 1
        )


def test_logger_info(caplog) -> None:
    runner = CliRunner()
    runner.invoke(cli, ["--dbt-project-dir", "dbt_project"])
    assert "Running dbt_bouncer (0.0.0)..." in caplog.text
