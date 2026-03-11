"""Checks related to column tests."""

import re
from typing import Any, Literal

from pydantic import Field, PrivateAttr

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import compile_pattern


class CheckColumnHasSpecifiedTest(BaseCheck):
    """Columns that match the specified regexp pattern must have a specified test.

    Parameters:
        column_name_pattern (str): Regex pattern to match the column name.
        test_name (str): Name of the test to check for.

    Receives:
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.
        tests (list[TestNode]): List of TestNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_column_has_specified_test
              column_name_pattern: ^is_.*
              test_name: not_null
        ```

    """

    catalog_node: Any | None = Field(default=None)
    column_name_pattern: str
    name: Literal["check_column_has_specified_test"]
    test_name: str

    _compiled_column_name_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_column_name_pattern = compile_pattern(
            self.column_name_pattern.strip()
        )

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If column does not have specified test.

        """
        catalog_node = self._require_catalog_node()
        columns_to_check = [
            v.name
            for _, v in catalog_node.columns.items()
            if self._compiled_column_name_pattern.match(str(v.name)) is not None
        ]
        tested_columns = set()
        for t in self._ctx.tests:
            test_metadata = getattr(t, "test_metadata", None)
            attached_node = getattr(t, "attached_node", None)
            if (
                test_metadata
                and attached_node
                and getattr(test_metadata, "name", None) == self.test_name
                and attached_node == catalog_node.unique_id
            ):
                tested_columns.add(getattr(t, "column_name", ""))
        non_complying_columns = [c for c in columns_to_check if c not in tested_columns]

        if non_complying_columns:
            raise DbtBouncerFailedCheckError(
                f"`{str(catalog_node.unique_id).split('.')[-1]}` has columns that should have a `{self.test_name}` test: {non_complying_columns}"
            )
