"""Checks related to model source code content and structure."""

import re
from typing import Any, Literal

from pydantic import Field, PrivateAttr

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


class CheckModelCodeDoesNotContainRegexpPattern(BaseCheck):
    """The raw code for a model must not match the specified regexp pattern.

    Parameters:
        regexp_pattern (str): The regexp pattern that should not be matched by the model code.

    Receives:
        model (ModelNode): The ModelNode object to check.

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

    model: Any | None = Field(default=None)
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
        model = self._require_model()
        if self._compiled_pattern.match(str(model.raw_code)) is not None:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(model.unique_id)}` contains a banned string: `{self.regexp_pattern}`."
            )


class CheckModelHardCodedReferences(BaseCheck):
    """A model must not contain hard-coded table references; use ref() or source() instead.

    Scans ``raw_code`` for patterns like ``FROM schema.table`` or
    ``JOIN catalog.schema.table`` that are not wrapped in Jinja expressions.
    Hard-coded references bypass the dbt DAG, break lineage, and are
    environment-specific.

    !!! warning

        This check is not foolproof and will not catch all hard-coded table
        references (e.g. references inside complex Jinja logic or comments).

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_hard_coded_references
        ```

    """

    model: Any | None = Field(default=None)
    name: Literal["check_model_hard_coded_references"]

    _jinja_pattern: re.Pattern[str] = PrivateAttr()
    _hard_coded_ref_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile regex patterns once at initialisation time."""
        # Strip Jinja blocks ({{ ... }}, {% ... %}) before scanning for bare refs
        object.__setattr__(
            self,
            "_jinja_pattern",
            re.compile(r"\{[{%].*?[%}]\}", re.DOTALL),
        )
        # Match FROM or JOIN followed by a dotted identifier (schema.table)
        # Only multi-part names (with a dot) are flagged; single-part CTE names are not
        object.__setattr__(
            self,
            "_hard_coded_ref_pattern",
            re.compile(r"\b(?:FROM|JOIN)\s+\w+\.\w+", re.IGNORECASE),
        )

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If the model contains hard-coded table references.

        """
        model = self._require_model()
        raw_code = model.raw_code or ""
        cleaned = self._jinja_pattern.sub("", raw_code)
        matches = self._hard_coded_ref_pattern.findall(cleaned)
        if matches:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(model.unique_id)}` contains hard-coded table "
                f"references: {matches}. Use `{{{{ ref(...) }}}}` or `{{{{ source(..., ...) }}}}` instead."
            )


class CheckModelHasSemiColon(BaseCheck):
    """Model may not end with a semi-colon (`;`).

    Receives:
        model (ModelNode): The ModelNode object to check.

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

    model: Any | None = Field(default=None)
    name: Literal["check_model_has_semi_colon"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model ends with a semi-colon.

        """
        model = self._require_model()
        raw_code = (model.raw_code or "").strip()
        if raw_code and raw_code[-1] == ";":
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(model.unique_id)}` ends with a semi-colon, this is not permitted."
            )


class CheckModelMaxNumberOfLines(BaseCheck):
    """Models may not have more than the specified number of lines.

    Parameters:
        max_number_of_lines (int): The maximum number of permitted lines.

        model (ModelNode): The ModelNode object to check.

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

    model: Any | None = Field(default=None)
    name: Literal["check_model_max_number_of_lines"]
    max_number_of_lines: int = Field(default=100)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If max lines exceeded.

        """
        model = self._require_model()
        actual_number_of_lines = (model.raw_code or "").count("\n") + 1

        if actual_number_of_lines > self.max_number_of_lines:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(model.unique_id)}` has {actual_number_of_lines} lines, this is more than the maximum permitted number of lines ({self.max_number_of_lines})."
            )
