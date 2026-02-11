from typing import TYPE_CHECKING, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import DbtBouncerTestBase


class CheckTestHasTags(BaseCheck):
    """Data tests must have the specified tags.

    Parameters:
        criteria (Literal["any", "all", "one"] | None): Whether the test must have any, all, or exactly one of the specified tags. Default: `any`.
        tags (list[str]): List of tags to check for.

    Receives:
        test (DbtBouncerTestBase): The DbtBouncerTestBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the test path. Test paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the test path. Only test paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_test_has_tags
              tags:
                - critical
        ```

    """

    criteria: Literal["any", "all", "one"] = Field(default="any")
    name: Literal["check_test_has_tags"]
    tags: list[str]
    test: "DbtBouncerTestBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If the test does not have the required tags.

        """
        if self.test is None:
            raise DbtBouncerFailedCheckError("self.test is None")
        test_tags = self.test.tags or []
        if self.criteria == "any":
            if not any(tag in test_tags for tag in self.tags):
                raise DbtBouncerFailedCheckError(
                    f"`{self.test.unique_id}` does not have any of the required tags: {self.tags}."
                )
        elif self.criteria == "all":
            missing_tags = [tag for tag in self.tags if tag not in test_tags]
            if missing_tags:
                raise DbtBouncerFailedCheckError(
                    f"`{self.test.unique_id}` is missing required tags: {missing_tags}."
                )
        elif self.criteria == "one" and sum(tag in test_tags for tag in self.tags) != 1:
            raise DbtBouncerFailedCheckError(
                f"`{self.test.unique_id}` must have exactly one of the required tags: {self.tags}."
            )
