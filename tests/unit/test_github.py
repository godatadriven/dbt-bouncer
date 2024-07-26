import pytest
import requests

from dbt_bouncer.github import call_github_api, create_requests_session
from dbt_bouncer.logger import logger


def test_create_requests_session():
    """
    Test that the GitHub API can be reached
    """
    assert isinstance(create_requests_session(), requests.Session)


def test_github_api_connection():
    """
    Test that the GitHub API can be reached
    """

    response = call_github_api(
        endpoint="user",  # Unfortunately this endpoint doesn't return a `success` key,
        method="GET",
    )
    logger.warning(response)
    assert isinstance(response["id"], int)


def test_github_api_connection_nonexisting_endpoint():
    """
    Test that the GitHub API can be reached
    """

    with pytest.raises(Exception):
        call_github_api(endpoint="I_don_t_exist", method="GET")
