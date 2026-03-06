"""Checks related to model descriptions and documentation coverage."""

import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import Nodes4
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
    )

from pydantic import ConfigDict, Field, PrivateAttr

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import (
    clean_path_str,
    compile_pattern,
    get_clean_model_name,
    is_description_populated,
)


class CheckModelDescriptionContainsRegexPattern(BaseCheck):
    """Models must have a description that matches the provided pattern.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        regexp_pattern (str): The regexp pattern that should match the model description.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_description_contains_regex_pattern
            - regex_pattern: .*pattern_to_match.*
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_description_contains_regex_pattern"]
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
            DbtBouncerFailedCheckError: If description does not match regex.

        """
        self._require_model()
        if not self._compiled_pattern.match(str(self.model.description)):
            raise DbtBouncerFailedCheckError(
                f"""`{get_clean_model_name(self.model.unique_id)}`'s description "{self.model.description}" doesn't match the supplied regex: {self.regexp_pattern}."""
            )


class CheckModelDescriptionPopulated(BaseCheck):
    """Models must have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

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
            - name: check_model_description_populated
        ```
        ```yaml
        manifest_checks:
            - name: check_model_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """

    min_description_length: int | None = Field(default=None)
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_description_populated"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If description is not populated.

        """
        self._require_model()
        if not self._is_description_populated(
            self.model.description or "", self.min_description_length
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` does not have a populated description."
            )


class CheckModelDocumentationCoverage(BaseCheck):
    """Set the minimum percentage of models that have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.
        min_model_documentation_coverage_pct (float): The minimum percentage of models that must have a populated description.

    Receives:
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_documentation_coverage
              min_model_documentation_coverage_pct: 90
        ```
        ```yaml
        manifest_checks:
            - name: check_model_documentation_coverage
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """

    model_config = ConfigDict(extra="forbid")

    description: str | None = Field(
        default=None,
        description="Description of what the check does and why it is implemented.",
    )
    index: int | None = Field(
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    min_model_documentation_coverage_pct: int = Field(
        default=100,
        ge=0,
        le=100,
    )
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_model_documentation_coverage"]
    severity: Literal["error", "warn"] | None = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If documentation coverage is less than minimum.

        """
        num_models = len(self.models)
        models_with_description = []
        for model in self.models:
            if is_description_populated(
                description=model.description or "", min_description_length=4
            ):
                models_with_description.append(model.unique_id)

        num_models_with_descriptions = len(models_with_description)
        model_description_coverage_pct = (
            num_models_with_descriptions / num_models
        ) * 100

        if model_description_coverage_pct < self.min_model_documentation_coverage_pct:
            raise DbtBouncerFailedCheckError(
                f"Only {model_description_coverage_pct}% of models have a populated description, this is less than the permitted minimum of {self.min_model_documentation_coverage_pct}%."
            )


class CheckModelDocumentedInSameDirectory(BaseCheck):
    """Models must be documented in the same directory where they are defined (i.e. `.yml` and `.sql` files are in the same directory).

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
            - name: check_model_documented_in_same_directory
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_documented_in_same_directory"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model is not documented in same directory.

        """
        self._require_model()
        model = cast("Nodes4", self.model)
        model_sql_path = Path(clean_path_str(model.original_file_path))
        model_sql_dir = model_sql_path.parent.parts

        if not (
            hasattr(model, "patch_path")
            and clean_path_str(model.patch_path or "") is not None
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(model.unique_id)}` is not documented."
            )

        patch_path_str = clean_path_str(model.patch_path or "")
        start_idx = patch_path_str.find("models")
        if start_idx != -1:
            patch_path_str = patch_path_str[start_idx:]

        model_doc_path = Path(patch_path_str)
        model_doc_dir = model_doc_path.parent.parts

        if model_doc_dir != model_sql_dir:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` is documented in a different directory to the `.sql` file: `{'/'.join(model_doc_dir)}` vs `{'/'.join(model_sql_dir)}`."
            )
