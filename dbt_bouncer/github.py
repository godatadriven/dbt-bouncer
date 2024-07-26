import json
import os
from functools import lru_cache
from typing import Dict, List, Literal, Optional

import requests
from requests import HTTPError
from requests.auth import AuthBase
from requests.structures import CaseInsensitiveDict

from dbt_bouncer.logger import logger
from dbt_bouncer.utils import make_markdown_table


class GitHubAuth(AuthBase):
    """
    Base object to authenticate against the GitHub API.
    """

    def __call__(self, r):
        r.headers = CaseInsensitiveDict(
            {
                "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        return r


def call_github_api(
    endpoint: str,
    method: Literal["GET", "POST"],
    data: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """Call the GitHub API with a GET request and return the response as a dict.

    Args:
        endpoint (str): Can contain "https://api.github.com/" in the beginning, or not.
        method (Literal["GET", "POST"]): GET or POST.
        data (Dict, optional): Defaults to None.
        params (Dict, optional): Defaults to None.

    Returns:
        Dict
    """

    if method == "GET":
        r = create_requests_session().get(
            auth=GitHubAuth(),
            params=params,
            url=f"https://api.github.com/{endpoint}",
        )
    elif method == "POST":
        r = create_requests_session().post(
            auth=GitHubAuth(),
            data=json.dumps(data),
            url=f"https://api.github.com/{endpoint}",
        )

    try:
        r.raise_for_status()
    except HTTPError as e:
        logger.error(f"{r.status_code=}")
        logger.error(f"{r.content=}")
        raise RuntimeError(e) from e

    logger.debug(f"Response: {r.status_code}, {r.reason}")

    return r.json()


@lru_cache
def create_requests_session() -> requests.Session:
    """
    Create a requests session and cache it to avoid recreating the session.
    """

    logger.debug("Creating reusable requests session...")
    return requests.Session()


def send_github_comment_failed_checks(failed_checks: List[List[str]]) -> None:
    """
    Send a GitHub comment to the PR with a list of failed checks.
    """

    md_formatted_comment = make_markdown_table(
        [["Check name", "Failure message"]] + list(sorted(failed_checks))
    )

    md_formatted_comment = f"## **Failed `dbt-bouncer`** checks\n\n{md_formatted_comment}\n\nSent from this [GitHub Action workflow run](https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']})."  # Would like to be more specific and include the job ID, but it's not exposed as an environment variable: https://github.com/actions/runner/issues/324

    logger.debug(f"{md_formatted_comment=}")

    r = call_github_api(
        data={"body": md_formatted_comment},
        endpoint=f"repos/{os.environ['GITHUB_REPOSITORY']}/issues/{os.environ['GITHUB_REF'].split('/')[-2]}/comments",
        method="POST",
    )
    logger.info(f"Comment URL: {r['url']}")
