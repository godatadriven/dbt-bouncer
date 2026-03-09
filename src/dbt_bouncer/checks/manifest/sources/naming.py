"""Checks related to source naming conventions."""

import re
from typing import Any, Literal

from pydantic import Field, PrivateAttr

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import compile_pattern


class CheckSourceNames(BaseCheck):
    """Sources must have a name that matches the supplied regex.

    Parameters:
        source_name_pattern (str): Regexp the source name must match.

    Receives:
        source (DbtBouncerSource): The DbtBouncerSourceBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_names
              source_name_pattern: >
                ^[a-z0-9_]*$
        ```

    """

    name: Literal["check_source_names"]
    source_name_pattern: str
    source: Any | None = Field(default=None)

    _compiled_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_pattern = compile_pattern(self.source_name_pattern.strip())

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If source name does not match regex.

        """
        source = self._require_source()
        if self._compiled_pattern.match(str(source.name)) is None:
            raise DbtBouncerFailedCheckError(
                f"`{source.source_name}.{source.name}` does not match the supplied regex `({self.source_name_pattern.strip()})`."
            )
