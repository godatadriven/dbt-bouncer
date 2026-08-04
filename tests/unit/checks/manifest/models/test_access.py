import re

import pytest

from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError
from dbt_bouncer.testing import _run_check, check_fails, check_passes


class TestCheckModelAccess:
    @pytest.mark.parametrize(
        ("access", "model_override", "check_fn"),
        [
            pytest.param(
                "private", {"access": "private"}, check_passes, id="private_access"
            ),
            pytest.param(
                "protected",
                {"access": "protected"},
                check_passes,
                id="protected_access",
            ),
            pytest.param(
                "public", {"access": "public"}, check_passes, id="public_access"
            ),
            pytest.param(
                "protected",
                {"access": "private"},
                check_fails,
                id="protected_vs_private",
            ),
            pytest.param(
                "public", {"access": "private"}, check_fails, id="public_vs_private"
            ),
            pytest.param(
                "private",
                {"access": "protected"},
                check_fails,
                id="private_vs_protected",
            ),
            pytest.param(
                "public", {"access": "protected"}, check_fails, id="public_vs_protected"
            ),
            pytest.param(
                "private", {"access": "public"}, check_fails, id="private_vs_public"
            ),
            pytest.param(
                "protected", {"access": "public"}, check_fails, id="protected_vs_public"
            ),
        ],
    )
    def test_check_model_access(self, access, model_override, check_fn):
        check_fn("check_model_access", access=access, model=model_override)

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

    def test_invalid_access_value_rejected(self):
        with pytest.raises(ValueError, match="'private', 'protected' or 'public'"):
            _run_check(
                "check_model_access", access="publik", model={"access": "public"}
            )

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
        ("model_override", "check_fn"),
        [
            pytest.param(
                {"access": "public", "contract": {"enforced": True}},
                check_passes,
                id="public_contract_enforced",
            ),
            pytest.param(
                {"access": "protected", "contract": {"enforced": False}},
                check_passes,
                id="protected_no_contract",
            ),
            pytest.param({}, check_passes, id="no_access_no_contract"),
            pytest.param({"access": None}, check_passes, id="null_access_no_contract"),
            pytest.param({"access": "private"}, check_passes, id="private_no_contract"),
            pytest.param(
                {"access": "public", "contract": {"enforced": True}, "columns": {}},
                check_passes,
                id="public_contract_enforced_empty_columns",
            ),
            pytest.param(
                {"access": "public", "contract": {"enforced": False}},
                check_fails,
                id="public_no_contract",
            ),
            pytest.param(
                {"access": "public"}, check_fails, id="public_contract_absent"
            ),
            pytest.param(
                {"access": "public", "contract": None},
                check_fails,
                id="public_contract_none",
            ),
            pytest.param(
                {"access": "public", "contract": {"enforced": None}},
                check_fails,
                id="public_contract_enforced_none",
            ),
        ],
    )
    def test_check_model_contract_enforced_for_public_model(
        self, model_override, check_fn
    ):
        check_fn("check_model_contract_enforced_for_public_model", model=model_override)

    def test_failure_message(self):
        check_fails(
            "check_model_contract_enforced_for_public_model",
            model={"access": "public", "contract": {"enforced": False}},
            match=r"`model_1` is a public model but does not have contracts enforced\.",
        )


class TestCheckModelGrantPrivilege:
    @pytest.mark.parametrize(
        ("privilege_pattern", "model_override", "check_fn"),
        [
            pytest.param(
                "select",
                {"config": {"grants": {"select": ["user1"]}}},
                check_passes,
                id="grant_select",
            ),
            pytest.param(
                # `re.match` is anchored at the start only, so an unanchored
                # pattern matches any grant that STARTS with it. Users must write
                # `^select$` for exact matching. This pins the prefix semantics and
                # would catch a future change from `re.match` to `re.fullmatch`.
                "select",
                {"config": {"grants": {"select_any_table": ["user1"]}}},
                check_passes,
                id="prefix_match_select_any_table",
            ),
            pytest.param(
                # The pattern is `.strip()`-ed before compiling, so surrounding
                # whitespace is ignored and behaves identically to "^select$".
                "  ^select$  ",
                {"config": {"grants": {"select": ["user1"]}}},
                check_passes,
                id="whitespace_stripped_pattern",
            ),
            pytest.param(
                "^(select|insert)$",
                {"config": {"grants": {"select": ["user1"], "insert": ["user2"]}}},
                check_passes,
                id="alternation_pattern",
            ),
            pytest.param(
                # A model with zero grants trivially complies, whether grants is
                # None, an empty dict, or config is absent entirely.
                "^select$",
                {"config": {"grants": None}},
                check_passes,
                id="grants_none",
            ),
            pytest.param(
                "^select$",
                {"config": {"grants": {}}},
                check_passes,
                id="grants_empty",
            ),
            pytest.param("^select$", {}, check_passes, id="config_absent"),
            pytest.param(
                "^select$", {"config": {}}, check_passes, id="grants_key_absent"
            ),
            # Grant keys are coerced with str() before matching, so a non-string
            # key is matched by its string form rather than raising.
            pytest.param(
                "^1$",
                {"config": {"grants": {1: ["user1"]}}},
                check_passes,
                id="non_string_grant_key_coerced",
            ),
            pytest.param(
                "^select$",
                {"config": {"grants": {"write": ["user1"]}}},
                check_fails,
                id="grant_write",
            ),
            pytest.param(
                # No `re.IGNORECASE` is applied, so `^select$` does not match the
                # grant `SELECT`.
                "^select$",
                {"config": {"grants": {"SELECT": ["user1"]}}},
                check_fails,
                id="case_sensitive_no_ignorecase",
            ),
            pytest.param(
                "^select$",
                {"config": {"grants": {1: ["user1"]}}},
                check_fails,
                id="non_string_grant_key_mismatch",
            ),
        ],
    )
    def test_check_model_grant_privilege(
        self, privilege_pattern, model_override, check_fn
    ):
        check_fn(
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
        ("privilege", "model_override", "check_fn"),
        [
            pytest.param(
                "select",
                {"config": {"grants": {"select": ["user1"]}}},
                check_passes,
                id="required_grant_present",
            ),
            pytest.param(
                # Only the grant KEY is checked, not the grantee list. A grant with
                # an empty grantee list still satisfies the requirement even though
                # nobody is actually granted anything — arguably a loophole.
                "select",
                {"config": {"grants": {"select": []}}},
                check_passes,
                id="present_empty_grantee_list",
            ),
            pytest.param(
                "select",
                {"config": {"grants": {"select": ["user1"], "insert": ["user2"]}}},
                check_passes,
                id="present_among_several",
            ),
            pytest.param(
                "select",
                {"config": {"grants": {"write": ["user1"]}}},
                check_fails,
                id="required_grant_missing",
            ),
            pytest.param(
                # Membership is an exact key match (`privilege not in grants`), NOT
                # a prefix/regex match. Contrast with check_model_grant_privilege,
                # where the pattern `select` DOES match `select_any_table`. This
                # pair documents the asymmetry between the two grant checks.
                "select",
                {"config": {"grants": {"select_any_table": ["user1"]}}},
                check_fails,
                id="exact_match_not_prefix",
            ),
            pytest.param(
                # Dict membership is case-sensitive, so `select` does not match the
                # grant key `SELECT`.
                "select",
                {"config": {"grants": {"SELECT": ["user1"]}}},
                check_fails,
                id="case_sensitive",
            ),
            pytest.param(
                # A model with no grants can't have the required one. `grants: None`
                # and `grants: {}` both fall through the `(grants or {})` guard...
                "select",
                {"config": {"grants": None}},
                check_fails,
                id="grants_none",
            ),
            pytest.param(
                "select",
                {"config": {"grants": {}}},
                check_fails,
                id="grants_empty",
            ),
            pytest.param(
                # ...and an absent config exercises the `config.grants if config`
                # branch, yielding the same result.
                "select",
                {},
                check_fails,
                id="config_absent",
            ),
            pytest.param("select", {"config": None}, check_fails, id="config_none"),
        ],
    )
    def test_check_model_grant_privilege_required(
        self, privilege, model_override, check_fn
    ):
        check_fn(
            "check_model_grant_privilege_required",
            privilege=privilege,
            model=model_override,
        )

    def test_failure_message(self):
        check_fails(
            "check_model_grant_privilege_required",
            privilege="select",
            model={"config": {"grants": {"write": ["user1"]}}},
            match=r"does not have the required grant privilege \(`select`\)\.",
        )


class TestCheckModelHasContractsEnforced:
    @pytest.mark.parametrize(
        ("model_override", "check_fn"),
        [
            pytest.param(
                {"contract": {"enforced": True}}, check_passes, id="enforced_true"
            ),
            pytest.param(
                {"contract": {"enforced": False}}, check_fails, id="enforced_false"
            ),
            pytest.param(
                # No `contract` key at all → `model.contract` is None → `not
                # model.contract` fails.
                {},
                check_fails,
                id="contract_absent",
            ),
            pytest.param({"contract": None}, check_fails, id="contract_none"),
            pytest.param(
                # `enforced: None` fails via `is not True`, which catches None as
                # well as False (a `!= True` regression would too, but this pins it).
                {"contract": {"enforced": None}},
                check_fails,
                id="enforced_none",
            ),
        ],
    )
    def test_check_model_has_contracts_enforced(self, model_override, check_fn):
        check_fn("check_model_has_contracts_enforced", model=model_override)

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
        ("max_n", "min_n", "model_override", "check_fn"),
        [
            pytest.param(
                1,
                1,
                {"config": {"grants": {"select": ["user1"]}}},
                check_passes,
                id="within_limits",
            ),
            pytest.param(
                # num_grants == max passes. The docstring calls max inclusive but
                # the code uses strict `>`, so this locks the inclusive boundary in.
                2,
                0,
                {"config": {"grants": {"select": ["u1"], "insert": ["u2"]}}},
                check_passes,
                id="at_max_boundary",
            ),
            pytest.param(
                # num_grants == min passes (min inclusive; code uses strict `<`).
                5,
                2,
                {"config": {"grants": {"select": ["u1"], "insert": ["u2"]}}},
                check_passes,
                id="at_min_boundary",
            ),
            pytest.param(
                # min == max: passes at exactly that count. The fail-either-side
                # cases are covered by `exceeds_max` and `below_min` below.
                2,
                2,
                {"config": {"grants": {"select": ["u1"], "insert": ["u2"]}}},
                check_passes,
                id="min_equals_max_exact",
            ),
            pytest.param(
                # Grants are counted by privilege (dict key), NOT by grantee. Three
                # grantees under one privilege count as 1, so max=1 passes.
                1,
                0,
                {"config": {"grants": {"select": ["u1", "u2", "u3"]}}},
                check_passes,
                id="counted_by_privilege_not_grantee",
            ),
            pytest.param(
                1,
                1,
                {"config": {"grants": {"select": ["user1"], "write": ["user1"]}}},
                check_fails,
                id="exceeds_max",
            ),
            pytest.param(
                2,
                2,
                {"config": {"grants": {"select": ["user1"]}}},
                check_fails,
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
                check_fails,
                id="one_above_max",
            ),
            pytest.param(
                # num_grants == min - 1 fails, with num < max so the min boundary
                # is isolated.
                5,
                2,
                {"config": {"grants": {"select": ["user1"]}}},
                check_fails,
                id="one_below_min",
            ),
        ],
    )
    def test_check_model_number_of_grants(self, max_n, min_n, model_override, check_fn):
        check_fn(
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
            pytest.param({"config": None}, id="config_none"),
        ],
    )
    def test_defaults_pass_with_zero_grants(self, model_override):
        # With no params, min defaults to 0, so zero grants passes. `(grants or {})`
        # handles a None grants value without erroring.
        check_passes("check_model_number_of_grants", model=model_override)

    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param({"config": {"grants": None}}, id="grants_none"),
            pytest.param({"config": None}, id="config_none"),
        ],
    )
    def test_min_1_fails_when_grants_absent(self, model_override):
        # A min of 1 with no grants fails rather than errors: missing grants count
        # as 0, confirming the `(grants or {})` guard treats None as empty.
        check_fails(
            "check_model_number_of_grants",
            min_number_of_privileges=1,
            model=model_override,
        )

    @pytest.mark.parametrize(
        ("max_n", "min_n", "match_pattern"),
        [
            pytest.param(1, -1, "greater than or equal to 0", id="min_negative"),
            pytest.param(0, 0, "greater than 0", id="max_zero"),
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

    @pytest.mark.parametrize(
        ("max_n", "min_n", "match_pattern"),
        [
            pytest.param(
                1,
                0,
                r"has more grants \(`2`\) than the specified maximum \(1\)\.",
                id="above_max",
            ),
            pytest.param(
                5,
                3,
                r"has less grants \(`2`\) than the specified minimum \(3\)\.",
                id="below_min",
            ),
        ],
    )
    def test_failure_messages(self, max_n, min_n, match_pattern):
        check_fails(
            "check_model_number_of_grants",
            max_number_of_privileges=max_n,
            min_number_of_privileges=min_n,
            model={"config": {"grants": {"select": ["u1"], "insert": ["u2"]}}},
            match=match_pattern,
        )
