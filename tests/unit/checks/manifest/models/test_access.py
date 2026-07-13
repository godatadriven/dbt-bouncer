import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelAccess:
    @pytest.mark.parametrize(
        ("access", "model_override"),
        [
            pytest.param("private", {"access": "private"}, id="private_access"),
            pytest.param("protected", {"access": "protected"}, id="protected_access"),
            pytest.param("public", {"access": "public"}, id="public_access"),
        ],
    )
    def test_pass(self, access, model_override):
        check_passes("check_model_access", access=access, model=model_override)

    @pytest.mark.parametrize(
        "access",
        [
            pytest.param("private", id="requested_private"),
            pytest.param("protected", id="requested_protected"),
            pytest.param("public", id="requested_public"),
        ],
    )
    def test_pass_when_model_has_no_access_attribute(self, access):
        # A model with no `access` attribute passes for ANY requested access value
        # because the check short-circuits on `if model.access`. This documents the
        # "Requires dbt 1.7+" fallback: older manifests (which lack `access`) do not
        # error. If this is ever considered a bug, this test forces the discussion.
        check_passes("check_model_access", access=access, model={})

    @pytest.mark.parametrize(
        ("access", "model_override"),
        [
            pytest.param("protected", {"access": "private"}, id="protected_vs_private"),
            pytest.param("public", {"access": "private"}, id="public_vs_private"),
            pytest.param("private", {"access": "protected"}, id="private_vs_protected"),
            pytest.param("public", {"access": "protected"}, id="public_vs_protected"),
            pytest.param("private", {"access": "public"}, id="private_vs_public"),
            pytest.param("protected", {"access": "public"}, id="protected_vs_public"),
        ],
    )
    def test_fail(self, access, model_override):
        check_fails("check_model_access", access=access, model=model_override)

    @pytest.mark.parametrize(
        ("access", "model_override", "match_pattern"),
        [
            pytest.param(
                "public",
                {"access": "protected"},
                r"`model_1` has `protected` access, it should have access `public`\.",
                id="protected_vs_public",
            ),
            pytest.param(
                "private",
                {"access": "public"},
                r"`model_1` has `public` access, it should have access `private`\.",
                id="public_vs_private",
            ),
        ],
    )
    def test_failure_message(self, access, model_override, match_pattern):
        from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError
        from dbt_bouncer.testing import _run_check

        with pytest.raises(DbtBouncerFailedCheckError, match=match_pattern) as exc_info:
            _run_check("check_model_access", access=access, model=model_override)

        # Uses the clean model name, not the full unique_id.
        assert "model.package_name.model_1" not in str(exc_info.value)


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
            pytest.param(
                {},
                id="no_access_no_contract",
            ),
            pytest.param(
                {"access": None},
                id="null_access_no_contract",
            ),
            pytest.param(
                {"access": "private"},
                id="private_no_contract",
            ),
            pytest.param(
                {"access": "public", "contract": {"enforced": True}, "columns": {}},
                id="public_contract_enforced_empty_columns",
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
            pytest.param(
                {"access": "public"},
                id="public_contract_absent",
            ),
            pytest.param(
                {"access": "public", "contract": None},
                id="public_contract_none",
            ),
            pytest.param(
                {"access": "public", "contract": {"enforced": None}},
                id="public_contract_enforced_none",
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
