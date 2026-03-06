"""Checks related to model access controls and contract enforcement."""

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
        self._require_model()
        if self.model.access and self.model.access.value != self.access:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has `{self.model.access.value}` access, it should have access `{self.access}`."
            )


class CheckModelContractEnforcedForPublicModel(BaseCheck):
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
        self._require_model()
        if (
            self.model.access
            and self.model.access.value == "public"
            and (not self.model.contract or self.model.contract.enforced is not True)
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` is a public model but does not have contracts enforced."
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

    _compiled_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_pattern = compile_pattern(self.privilege_pattern.strip())

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If grant privileges do not match regex.

        """
        self._require_model()
        config = self.model.config
        grants = config.grants if config else {}
        non_complying_grants = [
            i for i in (grants or {}) if self._compiled_pattern.match(str(i)) is None
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
        self._require_model()
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
        self._require_model()
        if not self.model.contract or self.model.contract.enforced is not True:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` does not have contracts enforced."
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
        self._require_model()
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
