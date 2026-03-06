from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.models.access import (
    CheckModelAccess,
    CheckModelContractEnforcedForPublicModel,
    CheckModelGrantPrivilege,
    CheckModelGrantPrivilegeRequired,
    CheckModelHasContractsEnforced,
    CheckModelNumberOfGrants,
)

_TEST_DATA_FOR_CHECK_MODEL_ACCESS = [
    pytest.param(
        "public",
        {
            "access": "public",
            "alias": "model_2",
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        does_not_raise(),
        id="public_access",
    ),
    pytest.param(
        "public",
        {
            "access": "protected",
            "alias": "model_2",
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="protected_access",
    ),
]


@pytest.mark.parametrize(
    ("access", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_ACCESS,
    indirect=["model"],
)
def test_check_model_access(access, model, expectation):
    with expectation:
        CheckModelAccess(
            access=access, model=model, name="check_model_access"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_CONTRACT_ENFORCED_FOR_PUBLIC_MODEL = [
    pytest.param(
        {
            "access": "public",
            "alias": "model_2",
            "contract": {"enforced": True},
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        does_not_raise(),
        id="public_contract_enforced",
    ),
    pytest.param(
        {
            "access": "protected",
            "alias": "model_2",
            "contract": {"enforced": False},
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        does_not_raise(),
        id="protected_no_contract",
    ),
    pytest.param(
        {
            "access": "public",
            "alias": "model_2",
            "contract": {"enforced": False},
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="public_no_contract",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_CONTRACT_ENFORCED_FOR_PUBLIC_MODEL,
    indirect=["model"],
)
def test_check_model_contract_enforced_for_public_model(model, expectation):
    with expectation:
        CheckModelContractEnforcedForPublicModel(
            model=model, name="check_model_contract_enforced_for_public_model"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_GRANT_PRIVILEGE = [
    pytest.param(
        "select",
        {
            "config": {"grants": {"select": ["user1"]}},
        },
        does_not_raise(),
        id="grant_select",
    ),
    pytest.param(
        "^select$",
        {
            "config": {"grants": {"write": ["user1"]}},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="grant_write",
    ),
]


@pytest.mark.parametrize(
    ("privilege_pattern", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_GRANT_PRIVILEGE,
    indirect=["model"],
)
def test_check_model_grant_privilege(privilege_pattern, model, expectation):
    with expectation:
        CheckModelGrantPrivilege(
            privilege_pattern=privilege_pattern,
            model=model,
            name="check_model_grant_privilege",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_GRANT_PRIVILEGE_REQUIRED = [
    pytest.param(
        "select",
        {
            "config": {"grants": {"select": ["user1"]}},
        },
        does_not_raise(),
        id="required_grant_present",
    ),
    pytest.param(
        "select",
        {
            "config": {"grants": {"write": ["user1"]}},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="required_grant_missing",
    ),
]


@pytest.mark.parametrize(
    ("privilege", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_GRANT_PRIVILEGE_REQUIRED,
    indirect=["model"],
)
def test_check_model_grant_privilege_required(privilege, model, expectation):
    with expectation:
        CheckModelGrantPrivilegeRequired(
            privilege=privilege,
            model=model,
            name="check_model_grant_privilege_required",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_CONTRACTS_ENFORCED = [
    pytest.param(
        {
            "contract": {"enforced": True},
        },
        does_not_raise(),
        id="contracts_enforced",
    ),
    pytest.param(
        {
            "contract": {"enforced": False},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="contracts_not_enforced",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_CONTRACTS_ENFORCED,
    indirect=["model"],
)
def test_check_model_has_contracts_enforced(model, expectation):
    with expectation:
        CheckModelHasContractsEnforced(
            model=model, name="check_model_has_contracts_enforced"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_NUMBER_OF_GRANTS = [
    pytest.param(
        1,
        1,
        {
            "config": {"grants": {"select": ["user1"]}},
        },
        does_not_raise(),
        id="within_limits",
    ),
    pytest.param(
        1,
        1,
        {
            "config": {"grants": {"select": ["user1"], "write": ["user1"]}},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_max",
    ),
    pytest.param(
        2,
        2,
        {
            "config": {"grants": {"select": ["user1"]}},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="below_min",
    ),
]


@pytest.mark.parametrize(
    ("max_number_of_privileges", "min_number_of_privileges", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_NUMBER_OF_GRANTS,
    indirect=["model"],
)
def test_check_model_number_of_grants(
    max_number_of_privileges, min_number_of_privileges, model, expectation
):
    with expectation:
        CheckModelNumberOfGrants(
            max_number_of_privileges=max_number_of_privileges,
            min_number_of_privileges=min_number_of_privileges,
            model=model,
            name="check_model_number_of_grants",
        ).execute()
