# mypy: disable-error-code="union-attr"
from typing import TYPE_CHECKING, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import NestedDict
from dbt_bouncer.utils import find_missing_meta_keys

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_common import (
        DbtBouncerTestBase,
    )


class CheckSingularTestHasMetaKeys(BaseCheck):
    """The `meta` config for singular tests must have the specified keys.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.
        singular_test (DbtBouncerTestBase): The DbtBouncerTestBase object to check.

    Other Parameters:
        description (Optional[str]): Description of what the check does and why it is implemented.
        exclude (Optional[str]): Regex pattern to match the test path. Test paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the test path. Only test paths that match the pattern will be checked.
        materialization (Optional[Literal["ephemeral", "incremental", "table", "view"]]): Limit check to tests with the specified materialization.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_singular_test_has_meta_keys
              keys:
                - maturity
                - owner
        ```

    """

    keys: NestedDict
    name: Literal["check_singular_test_has_meta_keys"]
    singular_test: "DbtBouncerTestBase" = Field(default=None)

    def execute(self) -> None:
        """Execute the check."""
        missing_keys = find_missing_meta_keys(
            meta_config=self.singular_test.test.meta,
            required_keys=self.keys.model_dump(),
        )
        assert missing_keys == [], (
            f"`{self.singular_test.unique_id.split('.')[2]}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
        )
