import re

import pytest

from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError
from dbt_bouncer.testing import _run_check, check_fails, check_passes


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
        # because the check short-circuits on `if model.access`. This is the intended
        # "Requires dbt 1.7+" fallback (see the check docstring): older manifests, which
        # lack `access`, are skipped rather than errored.
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
            pytest.param(
                # `re.match` is anchored at the start only, so an unanchored
                # pattern matches any grant that STARTS with it. Users must write
                # `^select$` for exact matching. This pins the prefix semantics and
                # would catch a future change from `re.match` to `re.fullmatch`.
                "select",
                {"config": {"grants": {"select_any_table": ["user1"]}}},
                id="prefix_match_select_any_table",
            ),
            pytest.param(
                # The pattern is `.strip()`-ed before compiling, so surrounding
                # whitespace is ignored and behaves identically to "^select$".
                "  ^select$  ",
                {"config": {"grants": {"select": ["user1"]}}},
                id="whitespace_stripped_pattern",
            ),
            pytest.param(
                "^(select|insert)$",
                {"config": {"grants": {"select": ["user1"], "insert": ["user2"]}}},
                id="alternation_pattern",
            ),
            pytest.param(
                # A model with zero grants trivially complies, whether grants is
                # None, an empty dict, or config is absent entirely.
                "^select$",
                {"config": {"grants": None}},
                id="grants_none",
            ),
            pytest.param(
                "^select$",
                {"config": {"grants": {}}},
                id="grants_empty",
            ),
            pytest.param(
                "^select$",
                {},
                id="config_absent",
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
            pytest.param(
                # No `re.IGNORECASE` is applied, so `^select$` does not match the
                # grant `SELECT`.
                "^select$",
                {"config": {"grants": {"SELECT": ["user1"]}}},
                id="case_sensitive_no_ignorecase",
            ),
        ],
    )
    def test_fail(self, privilege_pattern, model_override):
        check_fails(
            "check_model_grant_privilege",
            privilege_pattern=privilege_pattern,
            model=model_override,
        )

    def test_failure_message_lists_only_non_complying(self):
        # With a mix of complying and non-complying grants, only the non-complying
        # ones are reported. Guards against off-by-one filtering of the list.
        with pytest.raises(
            DbtBouncerFailedCheckError,
            match=r"don't comply with the specified regexp pattern \(\['write'\]\)",
        ) as exc_info:
            _run_check(
                "check_model_grant_privilege",
                privilege_pattern="^select$",
                model={"config": {"grants": {"select": ["user1"], "write": ["user2"]}}},
            )

        # The complying grant must not appear in the non-complying list.
        assert "'select'" not in str(exc_info.value)

    def test_invalid_regex_raises_re_error(self):
        # An invalid pattern surfaces as `re.error` (wrapped by `compile_pattern`),
        # not as a check failure. This pins which exception type reaches the user.
        with pytest.raises(re.error, match=r"Invalid regex pattern"):
            _run_check(
                "check_model_grant_privilege",
                privilege_pattern="[select",
                model={"config": {"grants": {"select": ["user1"]}}},
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
            pytest.param(
                # Only the grant KEY is checked, not the grantee list. A grant with
                # an empty grantee list still satisfies the requirement even though
                # nobody is actually granted anything — arguably a loophole.
                "select",
                {"config": {"grants": {"select": []}}},
                id="present_empty_grantee_list",
            ),
            pytest.param(
                "select",
                {"config": {"grants": {"select": ["user1"], "insert": ["user2"]}}},
                id="present_among_several",
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
            pytest.param(
                # Membership is an exact key match (`privilege not in grants`), NOT
                # a prefix/regex match. Contrast with check_model_grant_privilege,
                # where the pattern `select` DOES match `select_any_table`. This
                # pair documents the asymmetry between the two grant checks.
                "select",
                {"config": {"grants": {"select_any_table": ["user1"]}}},
                id="exact_match_not_prefix",
            ),
            pytest.param(
                # Dict membership is case-sensitive, so `select` does not match the
                # grant key `SELECT`.
                "select",
                {"config": {"grants": {"SELECT": ["user1"]}}},
                id="case_sensitive",
            ),
            pytest.param(
                # A model with no grants can't have the required one. `grants: None`
                # and `grants: {}` both fall through the `(grants or {})` guard...
                "select",
                {"config": {"grants": None}},
                id="grants_none",
            ),
            pytest.param(
                "select",
                {"config": {"grants": {}}},
                id="grants_empty",
            ),
            pytest.param(
                # ...and an absent config exercises the `config.grants if config`
                # branch, yielding the same result.
                "select",
                {},
                id="config_absent",
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

    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"contract": {"enforced": False}},
                id="enforced_false",
            ),
            pytest.param(
                # No `contract` key at all → `model.contract` is None → `not
                # model.contract` fails.
                {},
                id="contract_absent",
            ),
            pytest.param(
                {"contract": None},
                id="contract_none",
            ),
            pytest.param(
                # `enforced: None` fails via `is not True`, which catches None as
                # well as False (a `!= True` regression would too, but this pins it).
                {"contract": {"enforced": None}},
                id="enforced_none",
            ),
        ],
    )
    def test_fail(self, model_override):
        check_fails(
            "check_model_has_contracts_enforced",
            model=model_override,
        )

    def test_failure_message_uses_clean_model_name(self):
        with pytest.raises(
            DbtBouncerFailedCheckError,
            match=r"`model_1` does not have contracts enforced\.",
        ) as exc_info:
            _run_check(
                "check_model_has_contracts_enforced",
                model={"contract": {"enforced": False}},
            )

        # Uses the clean model name, not the full unique_id.
        assert "model.package_name.model_1" not in str(exc_info.value)


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
            pytest.param(
                # num_grants == max passes. The docstring calls max inclusive but
                # the code uses strict `>`, so this locks the inclusive boundary in.
                2,
                0,
                {"config": {"grants": {"select": ["u1"], "insert": ["u2"]}}},
                id="at_max_boundary",
            ),
            pytest.param(
                # num_grants == min passes (min inclusive; code uses strict `<`).
                5,
                2,
                {"config": {"grants": {"select": ["u1"], "insert": ["u2"]}}},
                id="at_min_boundary",
            ),
            pytest.param(
                # min == max: passes at exactly that count. The fail-either-side
                # cases are covered by `exceeds_max` and `below_min` below.
                2,
                2,
                {"config": {"grants": {"select": ["u1"], "insert": ["u2"]}}},
                id="min_equals_max_exact",
            ),
            pytest.param(
                # Grants are counted by privilege (dict key), NOT by grantee. Three
                # grantees under one privilege count as 1, so max=1 passes.
                1,
                0,
                {"config": {"grants": {"select": ["u1", "u2", "u3"]}}},
                id="counted_by_privilege_not_grantee",
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
            pytest.param(
                # num_grants == max + 1 fails, with min < num so the max boundary
                # is isolated.
                2,
                0,
                {
                    "config": {
                        "grants": {
                            "select": ["u1"],
                            "insert": ["u2"],
                            "update": ["u3"],
                        }
                    }
                },
                id="one_above_max",
            ),
            pytest.param(
                # num_grants == min - 1 fails, with num < max so the min boundary
                # is isolated.
                5,
                2,
                {"config": {"grants": {"select": ["user1"]}}},
                id="one_below_min",
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
        "model_override",
        [
            pytest.param({"config": {"grants": {}}}, id="grants_empty"),
            pytest.param({"config": {"grants": None}}, id="grants_none"),
        ],
    )
    def test_defaults_pass_with_zero_grants(self, model_override):
        # With no params, min defaults to 0, so zero grants passes. `(grants or {})`
        # handles a None grants value without erroring.
        check_passes("check_model_number_of_grants", model=model_override)

    def test_min_1_fails_when_grants_none(self):
        # A min of 1 with `grants: None` fails rather than errors: missing grants
        # count as 0, confirming the `(grants or {})` guard treats None as empty.
        check_fails(
            "check_model_number_of_grants",
            min_number_of_privileges=1,
            model={"config": {"grants": None}},
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
        with pytest.raises(ValueError, match=match_pattern):
            _run_check(
                "check_model_number_of_grants",
                max_number_of_privileges=max_n,
                min_number_of_privileges=min_n,
                model={"config": {"grants": {"select": ["user1"]}}},
            )
