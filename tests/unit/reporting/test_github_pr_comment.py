"""Tests for `dbt_bouncer.reporting.github_pr_comment`."""

from __future__ import annotations

import io
import logging
import urllib.error
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import orjson
import pytest

from dbt_bouncer.reporting import github_pr_comment
from dbt_bouncer.reporting.github_pr_comment import (
    build_marker,
    sync_github_pr_comment,
)

if TYPE_CHECKING:
    from pathlib import Path

MARKER = build_marker("dbt-bouncer.yml")
FAILED_RESULT = {
    "check_run_id": "check_model_names:0:model.a",
    "failure_message": "bad name",
    "file_path": "models/a.sql",
    "outcome": "failed",
    "severity": "error",
}
PASSED_RESULT = {
    "check_run_id": "check_model_names:0:model.b",
    "failure_message": None,
    "file_path": "models/b.sql",
    "outcome": "success",
    "severity": "error",
}


class FakeGitHub:
    """An in-memory stand-in for the GitHub issue comments API."""

    def __init__(self, comments: list[dict[str, Any]] | None = None) -> None:
        """Start with `comments` on the pull request."""
        self.comments = comments or []
        self.calls: list[tuple[str, str]] = []

    def __call__(
        self, method: str, url: str, _token: str, payload: dict | None = None
    ) -> Any:
        self.calls.append((method, url))
        body = (payload or {}).get("body")
        if method == "GET":
            page = int(url.rsplit("page=", 1)[1])
            return self.comments[(page - 1) * 100 : page * 100]
        if method == "POST":
            comment = {"body": body, "id": 1000 + len(self.comments)}
            self.comments.append(comment)
            return comment
        comment_id = int(url.rsplit("/", 1)[1])
        comment = next(c for c in self.comments if c["id"] == comment_id)
        comment["body"] = body
        return comment


@pytest.fixture
def pr_env(monkeypatch, tmp_path: Path) -> None:
    """Set the GitHub Actions variables of a pull request run."""
    event_file = tmp_path / "event.json"
    event_file.write_bytes(
        orjson.dumps({"pull_request": {"head": {"sha": "abc123"}, "number": 7}})
    )
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_API_URL", raising=False)
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_file))
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "99")
    monkeypatch.setenv("GITHUB_TOKEN", "token")


def _sync(fake: FakeGitHub, results: list[dict[str, Any]]) -> None:
    with patch.object(github_pr_comment, "_github_request", fake):
        sync_github_pr_comment(results, comment_key="dbt-bouncer.yml")


@pytest.mark.usefixtures("pr_env")
class TestSyncGithubPrComment:
    """Tests for `sync_github_pr_comment`."""

    def test_failure_creates_comment(self):
        """The first failing run creates a comment with the marker and the table."""
        fake = FakeGitHub()

        _sync(fake, [FAILED_RESULT, PASSED_RESULT])

        assert [m for m, _ in fake.calls] == ["GET", "POST"]
        assert (
            fake.calls[1][1]
            == "https://api.github.com/repos/owner/repo/issues/7/comments"
        )
        body = fake.comments[0]["body"]
        assert body.startswith(MARKER)
        assert "## **Failed `dbt-bouncer`** checks" in body
        assert "check_model_names:0:model.a" in body
        assert "check_model_names:0:model.b" not in body

    def test_failure_edits_existing_comment(self):
        """A later failing run edits the earlier comment, not a new one."""
        fake = FakeGitHub([{"body": f"{MARKER}\nold", "id": 5}])

        _sync(fake, [FAILED_RESULT])

        assert [m for m, _ in fake.calls] == ["GET", "PATCH"]
        assert fake.calls[1][1].endswith("/repos/owner/repo/issues/comments/5")
        assert len(fake.comments) == 1
        assert "bad name" in fake.comments[0]["body"]

    def test_pass_marks_existing_comment_as_passed(self):
        """A passing run edits the earlier comment to say the checks passed."""
        fake = FakeGitHub([{"body": f"{MARKER}\nold", "id": 5}])

        _sync(fake, [PASSED_RESULT])

        body = fake.comments[0]["body"]
        assert body.startswith(MARKER)
        assert "## **Passed `dbt-bouncer`** checks" in body
        assert "commit `abc123`" in body
        assert "https://github.com/owner/repo/actions/runs/99" in body

    def test_pass_without_existing_comment_posts_nothing(self):
        """A clean PR gets no comment."""
        fake = FakeGitHub()

        _sync(fake, [PASSED_RESULT])

        assert [m for m, _ in fake.calls] == ["GET"]
        assert fake.comments == []

    def test_ignores_other_markers_and_quotes(self):
        """Comments of another config, or that quote the marker, are not edited."""
        fake = FakeGitHub(
            [
                {"body": f"{build_marker('other.yml')}\nold", "id": 1},
                {"body": f"> {MARKER}\nquoted", "id": 2},
                {"body": None, "id": 3},
            ]
        )

        _sync(fake, [FAILED_RESULT])

        assert [m for m, _ in fake.calls] == ["GET", "POST"]

    def test_edits_latest_marked_comment_across_pages(self):
        """Comments are paged, and the latest marked comment is the one edited."""
        comments = [{"body": "noise", "id": i} for i in range(150)]
        comments[10]["body"] = f"{MARKER}\nolder"
        comments[120]["body"] = f"{MARKER}\nnewer"
        fake = FakeGitHub(comments)

        _sync(fake, [FAILED_RESULT])

        assert [m for m, _ in fake.calls] == ["GET", "GET", "PATCH"]
        assert fake.calls[2][1].endswith("/issues/comments/120")

    def test_uses_github_api_url(self, monkeypatch):
        """GitHub Enterprise Server is reached through `GITHUB_API_URL`."""
        monkeypatch.setenv("GITHUB_API_URL", "https://ghe.example.com/api/v3/")
        fake = FakeGitHub()

        _sync(fake, [FAILED_RESULT])

        assert fake.calls[0][1].startswith(
            "https://ghe.example.com/api/v3/repos/owner/repo/issues/7/comments?"
        )

    def test_http_error_is_a_warning(self, caplog):
        """A forbidden API call logs a hint and does not raise."""
        error = urllib.error.HTTPError("url", 403, "Forbidden", None, io.BytesIO())  # ty: ignore

        with (
            caplog.at_level(logging.WARNING),
            patch.object(
                github_pr_comment, "_github_request", MagicMock(side_effect=error)
            ),
        ):
            sync_github_pr_comment([FAILED_RESULT], comment_key="dbt-bouncer.yml")

        assert "HTTP 403" in caplog.text
        assert "pull-requests: write" in caplog.text

    def test_network_error_is_a_warning(self, caplog):
        """An unreachable API logs a warning and does not raise."""
        with (
            caplog.at_level(logging.WARNING),
            patch.object(
                github_pr_comment,
                "_github_request",
                MagicMock(side_effect=urllib.error.URLError("no route")),
            ),
        ):
            sync_github_pr_comment([FAILED_RESULT], comment_key="dbt-bouncer.yml")

        assert "Could not send the PR comment" in caplog.text


class TestReadPullRequest:
    """Tests for the environment checks before any API call."""

    @pytest.mark.parametrize(
        "missing", ["GITHUB_EVENT_PATH", "GITHUB_REPOSITORY", "GITHUB_TOKEN"]
    )
    @pytest.mark.usefixtures("pr_env")
    def test_missing_variable_skips(self, caplog, missing, monkeypatch):
        """Without the token, repository or event file, nothing is called."""
        monkeypatch.delenv(missing)
        fake = FakeGitHub()

        with caplog.at_level(logging.WARNING):
            _sync(fake, [FAILED_RESULT])

        assert fake.calls == []
        assert "No PR comment is sent" in caplog.text

    @pytest.mark.usefixtures("pr_env")
    def test_gh_token_is_accepted(self, monkeypatch):
        """`GH_TOKEN`, the variable the `gh` CLI reads, also works."""
        monkeypatch.delenv("GITHUB_TOKEN")
        monkeypatch.setenv("GH_TOKEN", "token")
        fake = FakeGitHub()

        _sync(fake, [FAILED_RESULT])

        assert [m for m, _ in fake.calls] == ["GET", "POST"]

    @pytest.mark.usefixtures("pr_env")
    def test_non_pull_request_event_skips(self, caplog, monkeypatch, tmp_path):
        """A push event has no pull request to comment on."""
        event_file = tmp_path / "push.json"
        event_file.write_bytes(orjson.dumps({"ref": "refs/heads/main"}))
        monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_file))
        fake = FakeGitHub()

        with caplog.at_level(logging.WARNING):
            _sync(fake, [FAILED_RESULT])

        assert fake.calls == []
        assert "not for a pull request" in caplog.text

    @pytest.mark.usefixtures("pr_env")
    def test_unreadable_event_file_skips(self, caplog, monkeypatch, tmp_path):
        """A missing event file logs a warning instead of raising."""
        monkeypatch.setenv("GITHUB_EVENT_PATH", str(tmp_path / "missing.json"))
        fake = FakeGitHub()

        with caplog.at_level(logging.WARNING):
            _sync(fake, [FAILED_RESULT])

        assert fake.calls == []
        assert "Could not read the GitHub event file" in caplog.text


class TestGithubRequest:
    """Tests for `_github_request`."""

    def test_refuses_non_https(self):
        """A `file:` or plain HTTP URL is refused before any request."""
        with pytest.raises(ValueError, match="non-HTTPS"):
            github_pr_comment._github_request("GET", "file:///etc/passwd", "token")

    def test_sends_json_with_auth_headers(self):
        """The request carries the token and the JSON payload."""
        response = MagicMock()
        response.read.return_value = b'{"id": 1}'
        response.__enter__.return_value = response

        with patch("urllib.request.urlopen", return_value=response) as mock_open:
            result = github_pr_comment._github_request(
                "POST", "https://api.github.com/x", "secret", {"body": "hi"}
            )

        assert result == {"id": 1}
        request = mock_open.call_args.args[0]
        assert request.get_method() == "POST"
        assert request.get_header("Authorization") == "Bearer secret"
        assert orjson.loads(request.data) == {"body": "hi"}


def test_build_marker_cannot_close_the_html_comment():
    """`--` in the key would end the HTML comment early, so it is removed."""
    assert build_marker("a---b.yml") == "<!-- dbt-bouncer-pr-comment: a-b.yml -->"
