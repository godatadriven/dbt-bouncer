"""Checks related to model file locations, names, and directory structure."""

import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
    )

from pydantic import ConfigDict, Field, PrivateAttr

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import clean_path_str, compile_pattern, get_clean_model_name


class CheckModelDirectories(BaseCheck):
    """Only specified sub-directories are permitted.

    Parameters:
        include (str): Regex pattern to the directory to check.
        permitted_sub_directories (list[str]): List of permitted sub-directories.

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
        - name: check_model_directories
          include: models
          permitted_sub_directories:
            - intermediate
            - marts
            - staging
        ```
        ```yaml
        # Restrict sub-directories within `./models/staging`
        - name: check_model_directories
          include: ^models/staging
          permitted_sub_directories:
            - crm
            - payments
        ```

    """

    include: str
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_directories"]
    permitted_sub_directories: list[str]

    _compiled_include: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_include = compile_pattern(self.include.strip().rstrip("/"))

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model located in `./models` or invalid subdirectory.

        """
        self._require_model()
        clean_path = clean_path_str(self.model.original_file_path)
        matched_path = self._compiled_include.match(clean_path)
        if matched_path is None:
            raise DbtBouncerFailedCheckError("matched_path is None")
        path_after_match = clean_path[matched_path.end() + 1 :]
        directory_to_check = Path(path_after_match).parts[0]

        if directory_to_check.replace(".sql", "") == self.model.name:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` is not located in a valid sub-directory ({self.permitted_sub_directories})."
            )
        else:
            if directory_to_check not in self.permitted_sub_directories:
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(self.model.unique_id)}` is located in the `{directory_to_check}` sub-directory, this is not a valid sub-directory ({self.permitted_sub_directories})."
                )


class CheckModelFileName(BaseCheck):
    r"""Models must have a file name that matches the supplied regex.

    Parameters:
        file_name_pattern (str): Regexp the file name must match. Please account for the `.sql` extension.

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
            - name: check_model_file_name
              description: Marts must include the model version in their file name.
              include: ^models/marts
              file_name_pattern: .*(v[0-9])\.sql$
        ```

    """

    file_name_pattern: str
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_file_name"]

    _compiled_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_pattern = compile_pattern(self.file_name_pattern.strip())

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If file name does not match regex.

        """
        self._require_model()
        file_name = Path(clean_path_str(self.model.original_file_path)).name
        if self._compiled_pattern.match(file_name) is None:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` is in a file that does not match the supplied regex `{self.file_name_pattern.strip()}`."
            )


class CheckModelPropertyFileLocation(BaseCheck):
    """Model properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

    Parameters:
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
            - name: check_model_property_file_location
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_property_file_location"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If property file location is incorrect.

        """
        self._require_model()
        if not (
            hasattr(self.model, "patch_path")
            and self.model.patch_path
            and clean_path_str(self.model.patch_path or "") is not None
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` is not documented."
            )

        original_path = Path(clean_path_str(self.model.original_file_path))
        relevant_parts = original_path.parts[1:-1]

        mapped_parts = []
        for part in relevant_parts:
            if part == "staging":
                mapped_parts.append("stg")
            elif part == "intermediate":
                mapped_parts.append("int")
            elif part == "marts":
                continue
            else:
                mapped_parts.append(part)

        expected_substr = "_".join(mapped_parts)
        properties_yml_name = Path(clean_path_str(self.model.patch_path or "")).name

        if not properties_yml_name.startswith("_"):
            raise DbtBouncerFailedCheckError(
                f"The properties file for `{get_clean_model_name(self.model.unique_id)}` (`{properties_yml_name}`) does not start with an underscore."
            )
        if expected_substr not in properties_yml_name:
            raise DbtBouncerFailedCheckError(
                f"The properties file for `{get_clean_model_name(self.model.unique_id)}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substr}`)."
            )
        if not properties_yml_name.endswith("__models.yml"):
            raise DbtBouncerFailedCheckError(
                f"The properties file for `{get_clean_model_name(self.model.unique_id)}` (`{properties_yml_name}`) does not end with `__models.yml`."
            )


class CheckModelSchemaName(BaseCheck):
    """Models must have a schema name that matches the supplied regex.

    Note that most setups will use schema names in development that are prefixed, for example:
        * dbt_jdoe_stg_payments
        * mary_stg_payments

    Please account for this if you wish to run `dbt-bouncer` against locally generated manifests.

    Parameters:
        schema_name_pattern (str): Regexp the schema name must match.

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
            - name: check_model_schema_name
              include: ^models/intermediate
              schema_name_pattern: .*intermediate # Accounting for schemas like `dbt_jdoe_intermediate`.
            - name: check_model_schema_name
              include: ^models/staging
              schema_name_pattern: .*stg_.*
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_schema_name"]
    schema_name_pattern: str

    _compiled_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_pattern = compile_pattern(self.schema_name_pattern.strip())

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If schema name does not match regex.

        """
        self._require_model()
        if self._compiled_pattern.match(str(self.model.schema_)) is None:
            raise DbtBouncerFailedCheckError(
                f"`{self.model.schema_}` does not match the supplied regex `{self.schema_name_pattern.strip()})`."
            )
