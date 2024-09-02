# mypy: disable-error-code="union-attr"

import logging
from typing import TYPE_CHECKING, List, Literal

import semver
from pydantic import Field

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    import warnings

    from dbt_bouncer.parsers import DbtBouncerManifest

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (
            UnitTests,
        )


class CheckUnitTestExpectFormats(BaseCheck):
    """Unit tests can only use the specified formats.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Parameters:
        permitted_formats (Optional[List[Literal["csv", "dict", "sql"]]]): A list of formats that are allowed to be used for `expect` input in a unit test.

    Receives:
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
        unit_test (UnitTests): The UnitTests object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Unit test paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Only unit test paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_unit_test_expect_format
              permitted_formats:
                - csv
        ```

    """

    manifest_obj: "DbtBouncerManifest" = Field(default=None)
    name: Literal["check_unit_test_expect_format"]
    permitted_formats: List[Literal["csv", "dict", "sql"]] = Field(
        default=["csv", "dict", "sql"],
    )
    unit_test: "UnitTests" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        if (
            semver.Version.parse(self.manifest_obj.manifest.metadata.dbt_version)
            >= "1.8.0"
        ):
            assert (
                self.unit_test.expect.format.value in self.permitted_formats
            ), f"Unit test `{self.unit_test.name}` has an `expect` format that is not permitted. Permitted formats are: {self.permitted_formats}."
        else:
            logging.warning(
                "The `check_unit_test_expect_format` check is only supported for dbt 1.8.0 and above.",
            )


class CheckUnitTestGivenFormats(BaseCheck):
    """Unit tests can only use the specified formats.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Parameters:
        permitted_formats (Optional[List[Literal["csv", "dict", "sql"]]]): A list of formats that are allowed to be used for `expect` input in a unit test.

    Receives:
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
        unit_test (UnitTests): The UnitTests object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Unit test paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Only unit test paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_unit_test_given_formats
              permitted_formats:
                - csv
        ```

    """

    manifest_obj: "DbtBouncerManifest" = Field(default=None)
    name: Literal["check_unit_test_given_formats"]
    permitted_formats: List[Literal["csv", "dict", "sql"]] = Field(
        default=["csv", "dict", "sql"],
    )
    unit_test: "UnitTests" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        if (
            semver.Version.parse(self.manifest_obj.manifest.metadata.dbt_version)
            >= "1.8.0"
        ):
            given_formats = [i.format.value for i in self.unit_test.given]
            assert all(
                e in self.permitted_formats for e in given_formats
            ), f"Unit test `{self.unit_test.name}` has given formats which are not permitted. Permitted formats are: {self.permitted_formats}."
        else:
            logging.warning(
                "The `check_unit_test_given_formats` check is only supported for dbt 1.8.0 and above.",
            )
