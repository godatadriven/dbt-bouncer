import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelAccess:
    @pytest.mark.parametrize(
        ("access", "model_override"),
        [
            pytest.param("public", {"access": "public"}, id="public_access"),
        ],
    )
    def test_pass(self, access, model_override):
        check_passes("check_model_access", access=access, model=model_override)

    @pytest.mark.parametrize(
        ("access", "model_override"),
        [
            pytest.param("public", {"access": "protected"}, id="protected_access"),
        ],
    )
    def test_fail(self, access, model_override):
        check_fails("check_model_access", access=access, model=model_override)


class TestCheckModelContractEnforcedForPublicModel:
    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"access": "public", "contract": {"enforced": True}},
                id="public_contract_enforced",
            ),
            pytest.param(
                {"access": "protected", "contract": {"enforced": False}},
                id="protected_no_contract",
            ),
        ],
    )
    def test_pass(self, model_override):
        check_passes(
            "check_model_contract_enforced_for_public_model", model=model_override
        )

    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"access": "public", "contract": {"enforced": False}},
                id="public_no_contract",
            ),
        ],
    )
    def test_fail(self, model_override):
        check_fails(
            "check_model_contract_enforced_for_public_model", model=model_override
        )


class TestCheckModelGrantPrivilege:
    @pytest.mark.parametrize(
        ("privilege_pattern", "model_override"),
        [
            pytest.param(
                "select",
                {"config": {"grants": {"select": ["user1"]}}},
                id="grant_select",
            ),
        ],
    )
    def test_pass(self, privilege_pattern, model_override):
        check_passes(
            "check_model_grant_privilege",
            privilege_pattern=privilege_pattern,
            model=model_override,
        )

    @pytest.mark.parametrize(
        ("privilege_pattern", "model_override"),
        [
            pytest.param(
                "^select$",
                {"config": {"grants": {"write": ["user1"]}}},
                id="grant_write",
            ),
        ],
    )
    def test_fail(self, privilege_pattern, model_override):
        check_fails(
            "check_model_grant_privilege",
            privilege_pattern=privilege_pattern,
            model=model_override,
        )


class TestCheckModelGrantPrivilegeRequired:
    @pytest.mark.parametrize(
        ("privilege", "model_override"),
        [
            pytest.param(
                "select",
                {"config": {"grants": {"select": ["user1"]}}},
                id="required_grant_present",
            ),
        ],
    )
    def test_pass(self, privilege, model_override):
        check_passes(
            "check_model_grant_privilege_required",
            privilege=privilege,
            model=model_override,
        )

    @pytest.mark.parametrize(
        ("privilege", "model_override"),
        [
            pytest.param(
                "select",
                {"config": {"grants": {"write": ["user1"]}}},
                id="required_grant_missing",
            ),
        ],
    )
    def test_fail(self, privilege, model_override):
        check_fails(
            "check_model_grant_privilege_required",
            privilege=privilege,
            model=model_override,
        )


class TestCheckModelHasContractsEnforced:
    def test_pass(self):
        check_passes(
            "check_model_has_contracts_enforced",
            model={"contract": {"enforced": True}},
        )

    def test_fail(self):
        check_fails(
            "check_model_has_contracts_enforced",
            model={"contract": {"enforced": False}},
        )


class TestCheckModelNumberOfGrants:
    @pytest.mark.parametrize(
        ("max_n", "min_n", "model_override"),
        [
            pytest.param(
                1,
                1,
                {"config": {"grants": {"select": ["user1"]}}},
                id="within_limits",
            ),
        ],
    )
    def test_pass(self, max_n, min_n, model_override):
        check_passes(
            "check_model_number_of_grants",
            max_number_of_privileges=max_n,
            min_number_of_privileges=min_n,
            model=model_override,
        )

    @pytest.mark.parametrize(
        ("max_n", "min_n", "model_override"),
        [
            pytest.param(
                1,
                1,
                {"config": {"grants": {"select": ["user1"], "write": ["user1"]}}},
                id="exceeds_max",
            ),
            pytest.param(
                2,
                2,
                {"config": {"grants": {"select": ["user1"]}}},
                id="below_min",
            ),
        ],
    )
    def test_fail(self, max_n, min_n, model_override):
        check_fails(
            "check_model_number_of_grants",
            max_number_of_privileges=max_n,
            min_number_of_privileges=min_n,
            model=model_override,
        )

    @pytest.mark.parametrize(
        ("max_n", "min_n", "match_pattern"),
        [
            pytest.param(1, -1, "greater than or equal to 0", id="min_negative"),
            pytest.param(0, 0, "greater than 0", id="max_zero"),
            pytest.param(
                1, -1, "greater than or equal to 0", id="min_negative_valid_max"
            ),
            pytest.param(1, 2, "must not exceed", id="min_exceeds_max"),
        ],
    )
    def test_raises_value_error_for_invalid_params(self, max_n, min_n, match_pattern):
        from dbt_bouncer.testing import _run_check

        with pytest.raises(ValueError, match=match_pattern):
            _run_check(
                "check_model_number_of_grants",
                max_number_of_privileges=max_n,
                min_number_of_privileges=min_n,
                model={"config": {"grants": {"select": ["user1"]}}},
            )
