"""Checks related to model source code content and structure."""

import re
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
    )

from pydantic import Field, PrivateAttr

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


class CheckModelCodeDoesNotContainRegexpPattern(BaseCheck):
    """The raw code for a model must not match the specified regexp pattern.

    Parameters:
        regexp_pattern (str): The regexp pattern that should not be matched by the model code.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            # Prefer `coalesce` over `ifnull`: https://docs.sqlfluff.com/en/stable/rules.html#sqlfluff.rules.sphinx.Rule_CV02
            - name: check_model_code_does_not_contain_regexp_pattern
              regexp_pattern: .*[i][f][n][u][l][l].*
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_code_does_not_contain_regexp_pattern"]
    regexp_pattern: str

    _compiled_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_pattern = compile_pattern(
            self.regexp_pattern.strip(), flags=re.DOTALL
        )

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If code contains banned string.

        """
        self._require_model()
        if self._compiled_pattern.match(str(self.model.raw_code)) is not None:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` contains a banned string: `{self.regexp_pattern}`."
            )


class CheckModelHasSemiColon(BaseCheck):
    """Model may not end with a semi-colon (`;`).

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_semi_colon
              include: ^models/marts
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_semi_colon"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model ends with a semi-colon.

        """
        self._require_model()
        raw_code = (self.model.raw_code or "").strip()
        if raw_code and raw_code[-1] == ";":
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` ends with a semi-colon, this is not permitted."
            )


class CheckModelMaxNumberOfLines(BaseCheck):
    """Models may not have more than the specified number of lines.

    Parameters:
        max_number_of_lines (int): The maximum number of permitted lines.

        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_number_of_lines
        ```
        ```yaml
        manifest_checks:
            - name: check_model_max_number_of_lines
              max_number_of_lines: 150
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_max_number_of_lines"]
    max_number_of_lines: int = Field(default=100)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If max lines exceeded.

        """
        self._require_model()
        actual_number_of_lines = (self.model.raw_code or "").count("\n") + 1

        if actual_number_of_lines > self.max_number_of_lines:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has {actual_number_of_lines} lines, this is more than the maximum permitted number of lines ({self.max_number_of_lines})."
            )
