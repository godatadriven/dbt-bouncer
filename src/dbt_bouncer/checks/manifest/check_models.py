import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import NestedDict
from dbt_bouncer.utils import (
    find_missing_meta_keys,
    get_package_version_number,
    is_description_populated,
)

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
        UnitTests,
    )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerExposureBase,
        DbtBouncerManifest,
        DbtBouncerModelBase,
        DbtBouncerTestBase,
    )

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import clean_path_str, get_clean_model_name


class CheckModelAccess(BaseCheck):
    """Models must have the specified access attribute. Requires dbt 1.7+.

    Parameters:
        access (Literal["private", "protected", "public"]): The access level to check for.

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
            # Align with dbt best practices that marts should be `public`, everything else should be `protected`
            - name: check_model_access
              access: protected
              include: ^models/intermediate
            - name: check_model_access
              access: public
              include: ^models/marts
            - name: check_model_access
              access: protected
              include: ^models/staging
        ```

    """

    access: Literal["private", "protected", "public"]
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_access"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If access is incorrect.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if self.model.access and self.model.access.value != self.access:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has `{self.model.access.value}` access, it should have access `{self.access}`."
            )


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

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If code contains banned string.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if (
            re.compile(self.regexp_pattern.strip(), flags=re.DOTALL).match(
                str(self.model.raw_code)
            )
            is not None
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` contains a banned string: `{self.regexp_pattern.strip()}`."
            )


class CheckModelContractsEnforcedForPublicModel(BaseCheck):
    """Public models must have contracts enforced.

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
            - name: check_model_contract_enforced_for_public_model
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_contract_enforced_for_public_model"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If contracts are not enforced for public model.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if (
            self.model.access
            and self.model.access.value == "public"
            and (not self.model.contract or self.model.contract.enforced is not True)
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` is a public model but does not have contracts enforced."
            )


class CheckModelDependsOnMacros(BaseCheck):
    """Models must depend on the specified macros.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the model must depend on any, all, or exactly one of the specified macros. Default: `any`.
        required_macros: (list[str]): List of macros the model must depend on. All macros must specify a namespace, e.g. `dbt.is_incremental`.

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
            - name: check_model_depends_on_macros
              required_macros:
                - dbt.is_incremental
            - name: check_model_depends_on_macros
              criteria: one
              required_macros:
                - my_package.sampler
                - my_package.sampling
        ```

    """

    criteria: Literal["any", "all", "one"] = Field(default="all")
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_depends_on_macros"]
    required_macros: list[str]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model does not depend on required macros.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        upstream_macros = [
            (".").join(m.split(".")[1:])
            for m in getattr(self.model.depends_on, "macros", []) or []
        ]
        if self.criteria == "any":
            if not any(macro in upstream_macros for macro in self.required_macros):
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(self.model.unique_id)}` does not depend on any of the required macros: {self.required_macros}."
                )
        elif self.criteria == "all":
            missing_macros = [
                macro for macro in self.required_macros if macro not in upstream_macros
            ]
            if missing_macros:
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(self.model.unique_id)}` is missing required macros: {missing_macros}."
                )
        elif (
            self.criteria == "one"
            and sum(macro in upstream_macros for macro in self.required_macros) != 1
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` must depend on exactly one of the required macros: {self.required_macros}."
            )


class CheckModelDependsOnMultipleSources(BaseCheck):
    """Models cannot reference more than one source.

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
            - name: check_model_depends_on_multiple_sources
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_depends_on_multiple_sources"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model references more than one source.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        num_reffed_sources = sum(
            x.split(".")[0] == "source"
            for x in getattr(self.model.depends_on, "nodes", []) or []
        )
        if num_reffed_sources > 1:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` references more than one source."
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

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If description does not match regex.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if not re.compile(self.regexp_pattern.strip(), flags=re.DOTALL).match(
            str(self.model.description)
        ):
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
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if not self._is_description_populated(
            self.model.description or "", self.min_description_length
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` does not have a populated description."
            )


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

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model located in `./models` or invalid subdirectory.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        clean_path = clean_path_str(self.model.original_file_path)
        matched_path = re.compile(self.include.strip().rstrip("/")).match(clean_path)
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
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        model_sql_path = Path(clean_path_str(self.model.original_file_path))
        model_sql_dir = model_sql_path.parent.parts

        if not (
            hasattr(self.model, "patch_path")
            and clean_path_str(self.model.patch_path or "") is not None
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` is not documented."
            )

        patch_path_str = clean_path_str(self.model.patch_path or "")
        start_idx = patch_path_str.find("models")
        if start_idx != -1:
            patch_path_str = patch_path_str[start_idx:]

        model_doc_path = Path(patch_path_str)
        model_doc_dir = model_doc_path.parent.parts

        if model_doc_dir != model_sql_dir:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` is documented in a different directory to the `.sql` file: `{'/'.join(model_doc_dir)}` vs `{'/'.join(model_sql_dir)}`."
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

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If file name does not match regex.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        file_name = Path(clean_path_str(self.model.original_file_path)).name
        if re.compile(self.file_name_pattern.strip()).match(file_name) is None:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` is in a file that does not match the supplied regex `{self.file_name_pattern.strip()}`."
            )


class CheckModelGrantPrivilege(BaseCheck):
    """Model can have grant privileges that match the specified pattern.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        privilege_pattern (str): Regex pattern to match the privilege.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_grant_privilege
              include: ^models/marts
              privilege_pattern: ^select
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_grant_privilege"]
    privilege_pattern: str

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If grant privileges do not match regex.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        config = self.model.config
        grants = config.grants if config else {}
        non_complying_grants = [
            i
            for i in (grants or {})
            if re.compile(self.privilege_pattern.strip()).match(str(i)) is None
        ]

        if non_complying_grants:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has grants (`{self.privilege_pattern}`) that don't comply with the specified regexp pattern ({non_complying_grants})."
            )


class CheckModelGrantPrivilegeRequired(BaseCheck):
    """Model must have the specified grant privilege.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        privilege (str): The privilege that is required.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_grant_privilege_required
              include: ^models/marts
              privilege: select
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_grant_privilege_required"]
    privilege: str

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If required grant privilege is missing.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        config = self.model.config
        grants = config.grants if config else {}
        if self.privilege not in (grants or {}):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` does not have the required grant privilege (`{self.privilege}`)."
            )


class CheckModelHasContractsEnforced(BaseCheck):
    """Model must have contracts enforced.

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
            - name: check_model_has_contracts_enforced
              include: ^models/marts
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_contracts_enforced"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If contracts are not enforced.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if not self.model.contract or self.model.contract.enforced is not True:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` does not have contracts enforced."
            )


class CheckModelHasExposure(BaseCheck):
    """Models must have an exposure.

    Receives:
        exposures (list[DbtBouncerExposureBase]):  List of DbtBouncerExposureBase objects parsed from `manifest.json`.
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
            - name: check_model_has_exposure
              description: Ensure all marts are part of an exposure.
              include: ^models/marts
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    exposures: list["DbtBouncerExposureBase"] = Field(default=[])
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_exposure"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model does not have an exposure.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        has_exposure = False
        for e in self.exposures:
            for m in getattr(e.depends_on, "nodes", []) or []:
                if m == self.model.unique_id:
                    has_exposure = True

        if not has_exposure:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` does not have an associated exposure."
            )


class CheckModelHasMetaKeys(BaseCheck):
    """The `meta` config for models must have the specified keys.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.
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
            - name: check_model_has_meta_keys
              keys:
                - maturity
                - owner
        ```

    """

    keys: NestedDict
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_meta_keys"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If required meta keys are missing.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        missing_keys = find_missing_meta_keys(
            meta_config=self.model.meta,
            required_keys=self.keys.model_dump(),
        )
        if missing_keys != []:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
            )


class CheckModelHasNoUpstreamDependencies(BaseCheck):
    """Identify if models have no upstream dependencies as this likely indicates hard-coded tables references.

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
            - name: check_model_has_no_upstream_dependencies
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_no_upstream_dependencies"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model has no upstream dependencies.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if (
            not self.model.depends_on
            or not self.model.depends_on.nodes
            or len(self.model.depends_on.nodes) <= 0
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has no upstream dependencies, this likely indicates hard-coded tables references."
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
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if (self.model.raw_code or "").strip()[-1] == ";":
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` ends with a semi-colon, this is not permitted."
            )


class CheckModelHasTags(BaseCheck):
    """Models must have the specified tags.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the model must have any, all, or exactly one of the specified tags. Default: `any`.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        tags (list[str]): List of tags to check for.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_tags
              tags:
                - tag_1
                - tag_2
        ```

    """

    criteria: Literal["any", "all", "one"] = Field(default="all")
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_tags"]
    tags: list[str]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model does not have required tags.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        model_tags = self.model.tags or []
        if self.criteria == "any":
            if not any(tag in model_tags for tag in self.tags):
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(self.model.unique_id)}` does not have any of the required tags: {self.tags}."
                )
        elif self.criteria == "all":
            missing_tags = [tag for tag in self.tags if tag not in model_tags]
            if missing_tags:
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(self.model.unique_id)}` is missing required tags: {missing_tags}."
                )
        elif (
            self.criteria == "one" and sum(tag in model_tags for tag in self.tags) != 1
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` must have exactly one of the required tags: {self.tags}."
            )


class CheckModelHasUniqueTest(BaseCheck):
    """Models must have a test for uniqueness of a column.

    Parameters:
        accepted_uniqueness_tests (list[str] | None): List of tests that are accepted as uniqueness tests.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        tests (list[DbtBouncerTestBase]): List of DbtBouncerTestBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_unique_test
              include: ^models/marts
        ```
        ```yaml
        manifest_checks:
        # Example of allowing a custom uniqueness test
            - name: check_model_has_unique_test
              accepted_uniqueness_tests:
                - dbt_expectations.expect_compound_columns_to_be_unique # i.e. tests from packages must include package name
                - my_custom_uniqueness_test
                - unique
        ```

    """

    accepted_uniqueness_tests: list[str] | None = Field(
        default=[
            "dbt_expectations.expect_compound_columns_to_be_unique",
            "dbt_utils.unique_combination_of_columns",
            "unique",
        ],
    )
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_unique_test"]
    tests: list["DbtBouncerTestBase"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model does not have a unique test.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        num_unique_tests = 0
        for test in self.tests:
            test_metadata = getattr(test, "test_metadata", None)
            attached_node = getattr(test, "attached_node", None)
            if (
                test_metadata
                and attached_node == self.model.unique_id
                and (
                    (
                        f"{getattr(test_metadata, 'namespace', '')}.{getattr(test_metadata, 'name', '')}"
                        in (self.accepted_uniqueness_tests or [])
                    )
                    or (
                        getattr(test_metadata, "namespace", None) is None
                        and getattr(test_metadata, "name", "")
                        in (self.accepted_uniqueness_tests or [])
                    )
                )
            ):
                num_unique_tests += 1
        if num_unique_tests < 1:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` does not have a test for uniqueness of a column."
            )


class CheckModelHasUnitTests(BaseCheck):
    """Models must have more than the specified number of unit tests.

    Parameters:
        min_number_of_unit_tests (int | None): The minimum number of unit tests that a model must have.

    Receives:
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        unit_tests (list[UnitTests]): List of UnitTests objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_unit_tests
              include: ^models/marts
        ```
        ```yaml
        manifest_checks:
            - name: check_model_has_unit_tests
              min_number_of_unit_tests: 2
        ```

    """

    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    min_number_of_unit_tests: int = Field(default=1)
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_unit_tests"]
    unit_tests: list["UnitTests"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model does not have enough unit tests.

        """
        if self.manifest_obj is None:
            raise DbtBouncerFailedCheckError("self.manifest_obj is None")
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if get_package_version_number(
            self.manifest_obj.manifest.metadata.dbt_version or "0.0.0"
        ) >= get_package_version_number("1.8.0"):
            num_unit_tests = len(
                [
                    t.unique_id
                    for t in self.unit_tests
                    if t.depends_on
                    and t.depends_on.nodes
                    and t.depends_on.nodes[0] == self.model.unique_id
                ],
            )
            if num_unit_tests < self.min_number_of_unit_tests:
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(self.model.unique_id)}` has {num_unit_tests} unit tests, this is less than the minimum of {self.min_number_of_unit_tests}."
                )
        else:
            logging.warning(
                "The `check_model_has_unit_tests` check is only supported for dbt 1.8.0 and above.",
            )


class CheckModelLatestVersionSpecified(BaseCheck):
    r"""Check that the `latest_version` attribute of the model is set.

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
            - name: check_model_latest_version_specified
              include: ^models/marts
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_latest_version_specified"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If latest version is not specified.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if self.model.latest_version is None:
            raise DbtBouncerFailedCheckError(
                f"`{self.model.name}` does not have a specified `latest_version`."
            )


class CheckModelMaxChainedViews(BaseCheck):
    """Models cannot have more than the specified number of upstream dependents that are not tables.

    Parameters:
        materializations_to_include (list[str] | None): List of materializations to include in the check.
        max_chained_views (int | None): The maximum number of upstream dependents that are not tables.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_chained_views
        ```
        ```yaml
        manifest_checks:
            - name: check_model_max_chained_views
              materializations_to_include:
                - ephemeral
                - my_custom_materialization
                - view
              max_chained_views: 5
        ```

    """

    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    materializations_to_include: list[str] = Field(
        default=["ephemeral", "view"],
    )
    max_chained_views: int = Field(
        default=3,
    )
    model: "DbtBouncerModelBase | None" = Field(default=None)
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_model_max_chained_views"]
    package_name: str | None = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If max chained views exceeded.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if self.manifest_obj is None:
            raise DbtBouncerFailedCheckError("self.manifest_obj is None")

        def return_upstream_view_models(
            materializations,
            max_chained_views,
            models,
            model_unique_ids_to_check,
            package_name,
            depth=0,
        ):
            """Recursive function to return model unique_id's of upstream models that are views. Depth of recursion can be specified. If no models meet the criteria then an empty list is returned.

            Returns
            -
                list[str]: List of model unique_id's of upstream models that are views.

            """
            if depth == max_chained_views or model_unique_ids_to_check == []:
                return model_unique_ids_to_check

            relevant_upstream_models = []
            for model in model_unique_ids_to_check:
                upstream_nodes = list(
                    next(m2 for m2 in models if m2.unique_id == model).depends_on.nodes,
                )
                if upstream_nodes != []:
                    upstream_models = [
                        m
                        for m in upstream_nodes
                        if m.split(".")[0] == "model"
                        and m.split(".")[1] == package_name
                    ]
                    for i in upstream_models:
                        if (
                            next(
                                m for m in models if m.unique_id == i
                            ).config.materialized
                            in materializations
                        ):
                            relevant_upstream_models.append(i)

            depth += 1
            return return_upstream_view_models(
                materializations=materializations,
                max_chained_views=max_chained_views,
                models=models,
                model_unique_ids_to_check=relevant_upstream_models,
                package_name=package_name,
                depth=depth,
            )

        if (
            len(
                return_upstream_view_models(
                    materializations=self.materializations_to_include,
                    max_chained_views=self.max_chained_views,
                    models=self.models,
                    model_unique_ids_to_check=[self.model.unique_id],
                    package_name=(
                        self.package_name
                        or self.manifest_obj.manifest.metadata.project_name
                    ),
                ),
            )
            != 0
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has more than {self.max_chained_views} upstream dependents that are not tables."
            )


class CheckModelMaxFanout(BaseCheck):
    """Models cannot have more than the specified number of downstream models.

    Parameters:
        max_downstream_models (int | None): The maximum number of permitted downstream models.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_fanout
              max_downstream_models: 2
        ```

    """

    max_downstream_models: int = Field(default=3)
    model: "DbtBouncerModelBase | None" = Field(default=None)
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_model_max_fanout"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If max fanout exceeded.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        num_downstream_models = sum(
            self.model.unique_id
            in (getattr(m.depends_on, "nodes", []) if m.depends_on else [])
            for m in self.models
        )

        if num_downstream_models > self.max_downstream_models:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has {num_downstream_models} downstream models, which is more than the permitted maximum of {self.max_downstream_models}."
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
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        actual_number_of_lines = (self.model.raw_code or "").count("\n") + 1

        if actual_number_of_lines > self.max_number_of_lines:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has {actual_number_of_lines} lines, this is more than the maximum permitted number of lines ({self.max_number_of_lines})."
            )


class CheckModelMaxUpstreamDependencies(BaseCheck):
    """Limit the number of upstream dependencies a model has.

    Parameters:
        max_upstream_macros (int | None): The maximum number of permitted upstream macros.
        max_upstream_models (int | None): The maximum number of permitted upstream models.
        max_upstream_sources (int | None): The maximum number of permitted upstream sources.

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
            - name: check_model_max_upstream_dependencies
              max_upstream_models: 3
        ```

    """

    max_upstream_macros: int = Field(
        default=5,
    )
    max_upstream_models: int = Field(
        default=5,
    )
    max_upstream_sources: int = Field(
        default=1,
    )
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_max_upstream_dependencies"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If max upstream dependencies exceeded.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        depends_on = self.model.depends_on
        if depends_on:
            num_upstream_macros = len(list(getattr(depends_on, "macros", []) or []))
            nodes = getattr(depends_on, "nodes", []) or []
            num_upstream_models = len(
                [m for m in nodes if m.split(".")[0] == "model"],
            )
            num_upstream_sources = len(
                [m for m in nodes if m.split(".")[0] == "source"],
            )
        else:
            num_upstream_macros = 0
            num_upstream_models = 0
            num_upstream_sources = 0

        if num_upstream_macros > self.max_upstream_macros:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has {num_upstream_macros} upstream macros, which is more than the permitted maximum of {self.max_upstream_macros}."
            )
        if num_upstream_models > self.max_upstream_models:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has {num_upstream_models} upstream models, which is more than the permitted maximum of {self.max_upstream_models}."
            )
        if num_upstream_sources > self.max_upstream_sources:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has {num_upstream_sources} upstream sources, which is more than the permitted maximum of {self.max_upstream_sources}."
            )


class CheckModelNames(BaseCheck):
    """Models must have a name that matches the supplied regex.

    Parameters:
        model_name_pattern (str): Regexp the model name must match.

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
            - name: check_model_names
              include: ^models/intermediate
              model_name_pattern: ^int_
            - name: check_model_names
              include: ^models/staging
              model_name_pattern: ^stg_
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_names"]
    model_name_pattern: str

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model name does not match regex.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if (
            re.compile(self.model_name_pattern.strip()).match(str(self.model.name))
            is None
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` does not match the supplied regex `{self.model_name_pattern.strip()}`."
            )


class CheckModelNumberOfGrants(BaseCheck):
    """Model can have the specified number of privileges.

    Receives:
        max_number_of_privileges (int | None): Maximum number of privileges, inclusive.
        min_number_of_privileges (int | None): Minimum number of privileges, inclusive.
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
            - name: check_model_number_of_grants
              include: ^models/marts
              max_number_of_privileges: 1 # Optional
              min_number_of_privileges: 0 # Optional
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_number_of_grants"]
    max_number_of_privileges: int = Field(default=100)
    min_number_of_privileges: int = Field(default=0)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If number of grants is not within limits.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        config = self.model.config
        grants = config.grants if config else {}
        num_grants = len((grants or {}).keys())

        if num_grants < self.min_number_of_privileges:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has less grants (`{num_grants}`) than the specified minimum ({self.min_number_of_privileges})."
            )
        if num_grants > self.max_number_of_privileges:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has more grants (`{num_grants}`) than the specified maximum ({self.max_number_of_privileges})."
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
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
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

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If schema name does not match regex.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if (
            re.compile(self.schema_name_pattern.strip()).match(str(self.model.schema_))
            is None
        ):
            raise DbtBouncerFailedCheckError(
                f"`{self.model.schema_}` does not match the supplied regex `{self.schema_name_pattern.strip()})`."
            )


class CheckModelVersionAllowed(BaseCheck):
    r"""Check that the version of the model matches the supplied regex pattern.

    Parameters:
        version_pattern (str): Regexp the version must match.

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
            - name: check_model_version_allowed
              include: ^models/marts
              version_pattern: >- # Versions must be numeric
                [0-9]\d*
            - name: check_model_version_allowed
              include: ^models/marts
              version_pattern: ^(stable|latest)$ # Version can be "stable" or "latest", nothing else is permitted
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_version_allowed"]
    version_pattern: str

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If version is not allowed.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if self.model.version and (
            re.compile(self.version_pattern.strip()).match(str(self.model.version))
            is None
        ):
            raise DbtBouncerFailedCheckError(
                f"Version `{self.model.version}` in `{self.model.name}` does not match the supplied regex `{self.version_pattern.strip()})`."
            )


class CheckModelVersionPinnedInRef(BaseCheck):
    r"""Check that the version of the model is always specified in downstream nodes.

    Receives:
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
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
            - name: check_model_version_pinned_in_ref
              include: ^models/marts
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_version_pinned_in_ref"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If version is not pinned in ref.

        """
        if self.model is None:
            raise DbtBouncerFailedCheckError("self.model is None")
        if self.manifest_obj is None:
            raise DbtBouncerFailedCheckError("self.manifest_obj is None")
        child_map = self.manifest_obj.manifest.child_map
        if child_map and self.model.unique_id in child_map:
            downstream_models = [
                x for x in child_map[self.model.unique_id] if x.startswith("model.")
            ]
        else:
            downstream_models = []

        downstream_models_with_unversioned_refs: list[str] = []
        for m in downstream_models:
            node = self.manifest_obj.manifest.nodes.get(m)
            refs = getattr(node, "refs", None)
            if node and refs and isinstance(refs, list):
                downstream_models_with_unversioned_refs.extend(
                    m
                    for ref in refs
                    if getattr(ref, "name", None) == self.model.unique_id.split(".")[-1]
                    and not getattr(ref, "version", None)
                )

        if downstream_models_with_unversioned_refs:
            raise DbtBouncerFailedCheckError(
                f"`{self.model.name}` is referenced without a pinned version in downstream models: {downstream_models_with_unversioned_refs}."
            )


class CheckModelsDocumentationCoverage(BaseModel):
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


class CheckModelsTestCoverage(BaseModel):
    """Set the minimum percentage of models that have at least one test.

    Parameters:
        min_model_test_coverage_pct (float): The minimum percentage of models that must have at least one test.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.
        tests (list[DbtBouncerTestBase]): List of DbtBouncerTestBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.


    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_test_coverage
              min_model_test_coverage_pct: 90
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
    name: Literal["check_model_test_coverage"]
    min_model_test_coverage_pct: float = Field(
        default=100,
        ge=0,
        le=100,
    )
    models: list["DbtBouncerModelBase"] = Field(default=[])
    severity: Literal["error", "warn"] | None = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )
    tests: list["DbtBouncerTestBase"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If test coverage is less than minimum.

        """
        num_models = len(self.models)
        models_with_tests = []
        for model in self.models:
            for test in self.tests:
                if test.depends_on and model.unique_id in getattr(
                    test.depends_on, "nodes", []
                ):
                    models_with_tests.append(model.unique_id)
        num_models_with_tests = len(set(models_with_tests))
        model_test_coverage_pct = (num_models_with_tests / num_models) * 100

        if model_test_coverage_pct < self.min_model_test_coverage_pct:
            raise DbtBouncerFailedCheckError(
                f"Only {model_test_coverage_pct}% of models have at least one test, this is less than the permitted minimum of {self.min_model_test_coverage_pct}%."
            )
