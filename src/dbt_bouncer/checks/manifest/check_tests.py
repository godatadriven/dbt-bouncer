from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_patterns import BaseHasTagsCheck


class CheckTestHasTags(BaseHasTagsCheck):
    """Data tests must have the specified tags.

    Parameters:
        criteria (Literal["any", "all", "one"] | None): Whether the test must have any, all, or exactly one of the specified tags. Default: `any`.
        tags (list[str]): List of tags to check for.

    Receives:
        test (TestNode): The TestNode object to check.

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
    test: Any | None = Field(default=None)

    @property
    def _resource_tags(self) -> list[str]:
        return self._require_test().tags or []

    @property
    def _resource_display_name(self) -> str:
        return str(self._require_test().unique_id)
