# mypy: disable-error-code="union-attr"

import warnings
from typing import List, Literal, Union

import pytest
from _pytest.fixtures import TopRequest
from pydantic import Field

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.utils import bouncer_check

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (
        UnitTests,
    )

import semver

from dbt_bouncer.logger import logger
from dbt_bouncer.parsers import DbtBouncerManifest


class CheckUnitTestExpectFormats(BaseCheck):
    name: Literal["check_unit_test_expect_format"]
    permitted_formats: List[Literal["csv", "dict", "sql"]] = Field(default=["csv", "dict", "sql"])


@pytest.mark.iterate_over_unit_tests
@bouncer_check
def check_unit_test_expect_format(
    manifest_obj: DbtBouncerManifest,
    request: TopRequest,
    permitted_formats: Union[List[Literal["csv", "dict", "sql"]], None] = None,
    unit_test: Union[UnitTests, None] = None,
    **kwargs,
) -> None:
    """
    Unit tests can only use the specified formats.

    Receives:
        exclude (Optional[str]): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Unit test paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Only unit test paths that match the pattern will be checked.
        permitted_formats (Optional[List[Literal["csv", "dict", "sql"]]]): A list of formats that are allowed to be used for `expect` input in a unit test.
        unit_test (UnitTests): The UnitTests object to check.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_unit_test_expect_format
              permitted_formats:
                - csv
        ```
    """

    if semver.Version.parse(manifest_obj.manifest.metadata.dbt_version) >= "1.8.0":
        assert (
            unit_test.expect.format.value in permitted_formats  # type: ignore[operator]
        ), f"Unit test `{unit_test.name}` has an `expect` format that is not permitted. Permitted formats are: {permitted_formats}."
    else:
        logger.warning(
            "The `check_unit_test_expect_format` check is only supported for dbt 1.8.0 and above."
        )


class CheckUnitTestGivenFormats(BaseCheck):
    name: Literal["check_unit_test_given_formats"]
    permitted_formats: List[Literal["csv", "dict", "sql"]] = Field(default=["csv", "dict", "sql"])


@pytest.mark.iterate_over_unit_tests
@bouncer_check
def check_unit_test_given_formats(
    manifest_obj: DbtBouncerManifest,
    request: TopRequest,
    permitted_formats: Union[List[Literal["csv", "dict", "sql"]], None] = None,
    unit_test: Union[UnitTests, None] = None,
    **kwargs,
) -> None:
    """
    Unit tests can only use the specified formats.

    Receives:
        exclude (Optional[str]): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Unit test paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Only unit test paths that match the pattern will be checked.
        permitted_formats (Optional[List[Literal["csv", "dict", "sql"]]]): A list of formats that are allowed to be used for `given` inputs in a unit test.
        unit_test (UnitTests): The UnitTests object to check.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_unit_test_given_formats
              permitted_formats:
                - csv
        ```
    """

    if semver.Version.parse(manifest_obj.manifest.metadata.dbt_version) >= "1.8.0":
        given_formats = [i.format.value for i in unit_test.given]
        assert all(
            e in permitted_formats for e in given_formats  # type: ignore[operator]
        ), f"Unit test `{unit_test.name}` has given formats which are not permitted. Permitted formats are: {permitted_formats}."
    else:
        logger.warning(
            "The `check_unit_test_given_formats` check is only supported for dbt 1.8.0 and above."
        )
