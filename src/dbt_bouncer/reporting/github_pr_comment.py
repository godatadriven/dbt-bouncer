"""Create or update one dbt-bouncer comment on a GitHub pull request.

Each run edits the same comment instead of adding a new one. A hidden HTML
marker at the start of the body identifies the comment. The marker includes a
key (the config file), so two dbt-bouncer jobs on one PR keep separate comments.

The GitHub API is called with `urllib` from the standard library, so the
feature adds no dependency. Any failure to post is logged as a warning and never
changes the exit code: the check result is what gates the PR, not the comment.
"""

from __future__ import annotations

import logging
import os
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import orjson

from dbt_bouncer.enums import CheckOutcome
from dbt_bouncer.utils import build_github_comment_body

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = ["build_marker", "sync_github_pr_comment"]

_COMMENTS_PER_PAGE = 100
_DEFAULT_API_URL = "https://api.github.com"
_REQUEST_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class _PullRequest:
    """The pull request the workflow run belongs to."""

    api_url: str
    head_sha: str
    number: int
    repository: str
    run_url: str | None
    token: str


def build_marker(comment_key: str) -> str:
    """Build the hidden marker that identifies the dbt-bouncer comment.

    Returns:
        str: An HTML comment, invisible in the rendered PR comment.

    """
    # `--` cannot appear inside an HTML comment, so it would end the marker early.
    return f"<!-- dbt-bouncer-pr-comment: {re.sub(r'-{2,}', '-', comment_key)} -->"


def _passed_body(pull_request: _PullRequest) -> str:
    """Build the comment body for a run where every check passed.

    Returns:
        str: The markdown comment body.

    """
    commit = f" as of commit `{pull_request.head_sha}`" if pull_request.head_sha else ""
    source = (
        f" Sent from this [GitHub Action workflow run]({pull_request.run_url})."
        if pull_request.run_url
        else ""
    )
    return f"## **Passed `dbt-bouncer`** checks\n\nAll checks passed{commit}.{source}"


def _read_pull_request() -> _PullRequest | None:
    """Read the pull request details from the GitHub Actions environment.

    Returns:
        _PullRequest | None: The pull request, or `None` if the run is not for a
            pull request or a required variable is missing. The reason is logged.

    """
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    repository = os.environ.get("GITHUB_REPOSITORY")
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not token or not repository or not event_path:
        logging.warning(
            "`--github-pr-comment` is set, but GITHUB_TOKEN (or GH_TOKEN), GITHUB_REPOSITORY or GITHUB_EVENT_PATH is not. No PR comment is sent. Pass `GITHUB_TOKEN: ${{ github.token }}` in the step's `env:`."
        )
        return None

    try:
        event = orjson.loads(Path(event_path).read_bytes())
    except (OSError, orjson.JSONDecodeError) as e:
        logging.warning(
            f"Could not read the GitHub event file `{event_path}`, so no PR comment is sent: {e}"
        )
        return None

    pull_request = event.get("pull_request") if isinstance(event, dict) else None
    if not isinstance(pull_request, dict) or "number" not in pull_request:
        logging.warning(
            "`--github-pr-comment` is set, but this workflow run is not for a pull request. No PR comment is sent."
        )
        return None

    run_id = os.environ.get("GITHUB_RUN_ID")
    server_url = os.environ.get("GITHUB_SERVER_URL", "https://github.com").rstrip("/")
    return _PullRequest(
        api_url=os.environ.get("GITHUB_API_URL", _DEFAULT_API_URL).rstrip("/"),
        head_sha=str((pull_request.get("head") or {}).get("sha") or ""),
        number=int(pull_request["number"]),
        repository=repository,
        run_url=f"{server_url}/{repository}/actions/runs/{run_id}" if run_id else None,
        token=token,
    )


def _github_request(
    method: str, url: str, token: str, payload: Mapping[str, Any] | None = None
) -> Any:
    """Send one request to the GitHub REST API.

    Returns:
        Any: The decoded JSON response.

    Raises:
        ValueError: If the URL is not HTTPS.

    """
    # GITHUB_API_URL comes from the environment, so refuse `file:` and other schemes.
    if not url.startswith("https://"):
        raise ValueError(f"Refusing to call a non-HTTPS GitHub API URL: `{url}`.")
    request = urllib.request.Request(  # ruff: ignore[suspicious-url-open-usage] - scheme checked above
        url,
        data=orjson.dumps(payload) if payload is not None else None,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method=method,
    )
    with urllib.request.urlopen(request, timeout=_REQUEST_TIMEOUT_SECONDS) as response:  # ruff: ignore[suspicious-url-open-usage] # nosec B310
        return orjson.loads(response.read() or b"null")


def _find_comment_id(pull_request: _PullRequest, marker: str) -> int | None:
    """Find the first comment on the pull request that starts with `marker`.

    Each run edits that one comment, so normally only one has the marker. Paging
    stops at the first match.

    Returns:
        int | None: The comment ID, or `None` if no comment has the marker.

    """
    page = 1
    while True:
        comments = _github_request(
            "GET",
            f"{pull_request.api_url}/repos/{pull_request.repository}/issues/{pull_request.number}/comments?per_page={_COMMENTS_PER_PAGE}&page={page}",
            pull_request.token,
        )
        for comment in comments:
            # `startswith`, not `in`: a reply that quotes the comment starts with `>`.
            if str(comment.get("body") or "").startswith(marker):
                return int(comment["id"])
        if len(comments) < _COMMENTS_PER_PAGE:
            return None
        page += 1


def sync_github_pr_comment(
    results: Sequence[Mapping[str, Any]],
    *,
    comment_key: str,
    show_all_failures: bool = False,
) -> None:
    """Create or update the dbt-bouncer comment on the current pull request.

    - If a check failed or warned, the comment lists those checks. An earlier
      comment from the same config is edited, else a new comment is created.
    - If every check passed, an earlier comment is edited to say so. If there is
      no earlier comment, nothing is posted, so clean PRs stay free of comments.

    Errors are logged as warnings and never raised.

    Args:
        results: The check results returned by the runner.
        comment_key: Identifies the comment, so separate configs keep separate comments.
        show_all_failures: List every failure, not only the first 25.

    """
    pull_request = _read_pull_request()
    if pull_request is None:
        return

    marker = build_marker(comment_key)
    failed_checks = [
        [
            str(r["check_run_id"]),
            str(r.get("file_path") or ""),
            str(r.get("severity", "")).lower(),
            str(r.get("failure_message", "")),
        ]
        for r in results
        if r["outcome"] != CheckOutcome.SUCCESS
    ]

    try:
        comment_id = _find_comment_id(pull_request, marker)
        if failed_checks:
            body = build_github_comment_body(failed_checks, show_all_failures)
        elif comment_id is not None:
            body = _passed_body(pull_request)
        else:
            logging.info("All checks passed and no earlier PR comment exists.")
            return

        payload = {"body": f"{marker}\n{body}"}
        if comment_id is None:
            _github_request(
                "POST",
                f"{pull_request.api_url}/repos/{pull_request.repository}/issues/{pull_request.number}/comments",
                pull_request.token,
                payload,
            )
            logging.info(f"Created a comment on PR #{pull_request.number}.")
        else:
            _github_request(
                "PATCH",
                f"{pull_request.api_url}/repos/{pull_request.repository}/issues/comments/{comment_id}",
                pull_request.token,
                payload,
            )
            logging.info(f"Updated comment {comment_id} on PR #{pull_request.number}.")
    except urllib.error.HTTPError as e:
        hint = (
            " Give the job `permissions: pull-requests: write`."
            if e.code in {401, 403, 404}
            else ""
        )
        logging.warning(
            f"Could not send the PR comment: GitHub API returned HTTP {e.code}.{hint}"
        )
    except (OSError, ValueError) as e:
        logging.warning(f"Could not send the PR comment: {e}")
