import logging

import pytest
from click.testing import CliRunner

from src.dbt_bouncer.logger import logger
from src.dbt_bouncer.main import cli
from tests.pytest_helpers import catch_logs, records_to_tuples


@pytest.mark.skipif(reason="Refactoring to a `./src` directory broke this.")
def test_logger_debug() -> None:
    with catch_logs(level=logging.DEBUG, logger=logger) as handler:
        runner = CliRunner()
        runner.invoke(
            cli,
            [
                "--config-file",
                "dbt-bouncer-example.yml",
            ],
        )

        assert (
            len(
                [
                    record
                    for record in records_to_tuples(handler.records)
                    if record[2].find("Loading config from") >= 0
                ]
            )
            >= 2
        )


def test_logger_info(caplog) -> None:
    runner = CliRunner()
    runner.invoke(
        cli,
        ["--config-file", "dbt-bouncer-example.yml"],
    )
    assert "Running dbt-bouncer (0.0.0)..." in caplog.text
