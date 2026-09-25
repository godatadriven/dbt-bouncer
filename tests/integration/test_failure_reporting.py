"""Integration tests for how failures are shown: console, verbosity and PR comment.

The console table and the GitHub PR comment both cap the failures they list at
25, unless `--show-all-failures` is passed. Two copies of a check that fails on
all 14 models give 28 failures, enough to hit that cap.
"""

import re
from pathlib import PurePath

import pytest

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app
from tests.integration.constants import FAILING_CHECK

_COMMENT_FILE = "github-comment.md"
_COMMENT_HEADER = "## **Failed `dbt-bouncer`** checks"
_TRUNCATION_LOG = "More than 25 checks failed"
_TRUNCATION_NOTE = "**Note:** Only the first 25 failed checks (of 28) are shown."

_PASSING_CHECK = {
    "name": "check_model_names",
    "include": r"models/staging/crm/stg_orders\.sql",
    "model_name_pattern": "^stg_",
}

_SHOW_ALL = pytest.mark.parametrize(
    "show_all_failures", [False, True], ids=["default", "show_all_failures"]
)


@pytest.fixture(autouse=True)
def _comment_file_in_tmp_path(monkeypatch, tmp_path):
    """Write any PR comment into `tmp_path`, and log at the default level.

    The comment goes to `/app/github-comment.md` when `GITHUB_REPOSITORY` is set,
    which it is on GitHub Actions, and to `./github-comment.md` otherwise.
    """
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.chdir(tmp_path)


def _run(cli_runner, config_file, *args, **kwargs):
    """Invoke `dbt-bouncer run` against `config_file`.

    Returns:
        Result: The `CliRunner` result.

    """
    return cli_runner.invoke(
        app,
        ["run", "--config-file", PurePath(config_file).as_posix(), *args],
        **kwargs,
    )


def _comment_rows(tmp_path) -> list[str]:
    """Return the failure rows of the PR comment table.

    Returns:
        list[str]: One line per listed failure.

    """
    comment = (tmp_path / _COMMENT_FILE).read_text(encoding="utf-8")
    return [line for line in comment.splitlines() if line.startswith("| check_")]


@_SHOW_ALL
def test_console_caps_failures_unless_show_all_failures(
    caplog, cli_runner, show_all_failures, write_checks_config
):
    """The console lists 25 of 28 failures and says so; `--show-all-failures` lists all 28."""
    config_file = write_checks_config([FAILING_CHECK[0], FAILING_CHECK[0]])

    # A wide terminal keeps each check name on one line so rows can be counted.
    result = _run(
        cli_runner,
        config_file,
        *(["--show-all-failures"] if show_all_failures else []),
        env={"COLUMNS": "250"},
    )

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    rows = re.findall(r"check_model_names:[01]:\w+", result.output)
    assert len(rows) == (28 if show_all_failures else 25)
    assert (_TRUNCATION_LOG in caplog.text) is not show_all_failures


def test_console_does_not_cap_below_25_failures(
    caplog, cli_runner, write_checks_config
):
    """14 failures are all listed without the truncation hint."""
    result = _run(
        cli_runner, write_checks_config(FAILING_CHECK), env={"COLUMNS": "250"}
    )

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    assert len(re.findall(r"check_model_names:0:\w+", result.output)) == 14
    assert _TRUNCATION_LOG not in caplog.text


@pytest.mark.parametrize(
    ("verbosity_args", "expect_debug"),
    [([], False), (["-v"], True), (["--verbosity"], True)],
    ids=["default", "short_flag", "long_flag"],
)
def test_verbosity_enables_debug_logging(
    caplog, cli_runner, expect_debug, verbosity_args, write_checks_config
):
    """Debug records, such as the full failed-check list, appear only with `-v`."""
    result = _run(cli_runner, write_checks_config(FAILING_CHECK), *verbosity_args)

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    assert ("failed_checks=" in caplog.text) is expect_debug
    assert any(r.levelname == "DEBUG" for r in caplog.records) is expect_debug


def test_no_pr_comment_file_by_default(cli_runner, tmp_path, write_checks_config):
    """Without the flag, a failing run writes no PR comment."""
    result = _run(cli_runner, write_checks_config(FAILING_CHECK))

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    assert not (tmp_path / _COMMENT_FILE).exists()


def test_no_pr_comment_file_when_every_check_passes(
    cli_runner, tmp_path, write_checks_config
):
    """There is nothing to comment on a passing run, so no file is written."""
    result = _run(
        cli_runner, write_checks_config([_PASSING_CHECK]), "--create-pr-comment-file"
    )

    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert not (tmp_path / _COMMENT_FILE).exists()


@pytest.mark.parametrize(
    ("args", "env"),
    [
        (["--create-pr-comment-file"], {}),
        ([], {"DBT_BOUNCER_CREATE_PR_COMMENT_FILE": "true"}),
    ],
    ids=["flag", "env_var"],
)
def test_pr_comment_file_lists_the_failure(
    args, cli_runner, env, tmp_path, write_checks_config
):
    """The PR comment holds a markdown table row per failure: name, file and message."""
    check = {**FAILING_CHECK[0], "include": r"models/marts/finance/orders\.sql"}

    result = _run(cli_runner, write_checks_config([check]), *args, env=env)

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    comment = (tmp_path / _COMMENT_FILE).read_text(encoding="utf-8")
    assert comment.startswith(_COMMENT_HEADER)
    assert "| Check name | File | Failure message |" in comment
    assert _comment_rows(tmp_path) == [
        "| check_model_names:0:orders | models/marts/finance/orders.sql | "
        "`orders` does not match the supplied regex `^zzz_`. |"
    ]
    assert "**Note:**" not in comment


@_SHOW_ALL
def test_pr_comment_caps_failures_unless_show_all_failures(
    cli_runner, show_all_failures, tmp_path, write_checks_config
):
    """The comment lists 25 of 28 failures with a note; `--show-all-failures` lists all 28."""
    config_file = write_checks_config([FAILING_CHECK[0], FAILING_CHECK[0]])

    result = _run(
        cli_runner,
        config_file,
        "--create-pr-comment-file",
        *(["--show-all-failures"] if show_all_failures else []),
    )

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    rows = _comment_rows(tmp_path)
    assert len(rows) == (28 if show_all_failures else 25)
    # Rows are sorted, so the listed ones are always the same.
    assert rows == sorted(rows)
    comment = (tmp_path / _COMMENT_FILE).read_text(encoding="utf-8")
    assert (_TRUNCATION_NOTE in comment) is not show_all_failures
