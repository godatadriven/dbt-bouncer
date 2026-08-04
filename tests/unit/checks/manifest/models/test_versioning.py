import re

import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelLatestVersionSpecified:
    @pytest.mark.parametrize(
        ("model", "check_fn"),
        [
            pytest.param(
                {"latest_version": 2}, check_passes, id="latest_version_integer"
            ),
            pytest.param(
                {"latest_version": "stable"}, check_passes, id="latest_version_string"
            ),
            # `latest_version` is compared against None, so a falsy-but-set value
            # is still a specified version.
            pytest.param({"latest_version": 0}, check_passes, id="latest_version_zero"),
            pytest.param(
                {"latest_version": ""}, check_passes, id="latest_version_empty_string"
            ),
            pytest.param(
                {"latest_version": None}, check_fails, id="latest_version_none"
            ),
            pytest.param({}, check_fails, id="latest_version_absent"),
        ],
    )
    def test_check_model_latest_version_specified(self, model, check_fn):
        check_fn("check_model_latest_version_specified", model=model)

    def test_failure_message_names_the_model(self):
        check_fails(
            "check_model_latest_version_specified",
            model={"latest_version": None, "name": "customers"},
            match=r"`customers` does not have a specified `latest_version`",
        )


class TestCheckModelVersionAllowed:
    @pytest.mark.parametrize(
        ("version", "version_pattern", "check_fn"),
        [
            pytest.param(0, r"[0-9]\d*", check_passes, id="integer_version_0"),
            pytest.param(1, r"[0-9]\d*", check_passes, id="integer_version_1"),
            pytest.param(10, r"[0-9]\d*", check_passes, id="integer_version_10"),
            pytest.param(100, r"[0-9]\d*", check_passes, id="integer_version_100"),
            pytest.param(
                "stable", r"^(stable|latest)$", check_passes, id="string_version_stable"
            ),
            pytest.param(
                "latest", r"^(stable|latest)$", check_passes, id="string_version_latest"
            ),
            # An unversioned model has nothing to validate, so the pattern is
            # never applied.
            pytest.param(
                None, r"^never_matches$", check_passes, id="unversioned_model_skipped"
            ),
            pytest.param(
                "anything", "", check_passes, id="empty_pattern_matches_everything"
            ),
            pytest.param(
                "STABLE",
                r"(?i)^stable$",
                check_passes,
                id="inline_ignorecase_flag_honoured",
            ),
            pytest.param("golden", r"[0-9]\d*", check_fails, id="non_numeric_version"),
            pytest.param(
                2, r"^(stable|latest)$", check_fails, id="numeric_version_not_allowed"
            ),
            # `version` is compared against None, not truthiness, so a version
            # of 0 is validated like any other rather than silently skipped.
            pytest.param(
                0, r"^never_matches$", check_fails, id="version_zero_is_validated"
            ),
            pytest.param(
                "stable",
                r"(?i)^STABLE$X",
                check_fails,
                id="inline_flag_still_must_match",
            ),
        ],
    )
    def test_check_model_version_allowed(self, version, version_pattern, check_fn):
        check_fn(
            "check_model_version_allowed",
            model={"version": version},
            version_pattern=version_pattern,
        )

    def test_integer_version_coerced_to_string_before_matching(self):
        check_passes(
            "check_model_version_allowed",
            model={"version": 2},
            version_pattern=r"^2$",
        )

    def test_pattern_is_start_anchored_not_a_full_match(self):
        # The check uses `re.match`, which anchors at the start only, so a
        # trailing suffix is accepted.
        check_passes(
            "check_model_version_allowed",
            model={"version": "2abc"},
            version_pattern=r"[0-9]",
        )

    def test_pattern_whitespace_stripped_before_matching(self):
        # Without stripping, the surrounding whitespace would prevent a match.
        check_passes(
            "check_model_version_allowed",
            model={"version": "stable"},
            version_pattern="  ^stable$  ",
        )

    def test_failure_message_reports_version_name_and_stripped_pattern(self):
        check_fails(
            "check_model_version_allowed",
            model={"name": "customers", "version": "golden"},
            version_pattern="  ^stable$  ",
            match=(
                r"Version `golden` in `customers` does not match the supplied "
                r"regex `\^stable\$`\."
            ),
        )

    def test_invalid_regex_raises_re_error(self):
        check_fails(
            "check_model_version_allowed",
            model={"version": "1"},
            version_pattern="[unterminated",
            expected_exception=re.error,
        )


# A versioned model as dbt actually emits it: `unique_id` carries the version
# suffix while `name` does not.
_MODEL_V2 = {
    "latest_version": 2,
    "name": "customers",
    "unique_id": "model.package_name.customers.v2",
    "version": 2,
}

_CHILD_MAP_V2 = {"model.package_name.customers.v2": ["model.package_name.orders"]}


def _downstream_model(name: str, refs: list[dict] | None) -> dict:
    """Build a minimal downstream model node carrying the given `refs`.

    Args:
        name: The model name, also used to derive its `unique_id`.
        refs: The `refs` entries for the node, or None to omit the key entirely.

    Returns:
        dict: A manifest node dict.

    """
    node = {
        "name": name,
        "resource_type": "model",
        "unique_id": f"model.package_name.{name}",
    }
    if refs is not None:
        node["refs"] = refs
    return node


def _manifest(child_map: dict | None, nodes: dict) -> dict:
    return {"child_map": child_map, "nodes": nodes}


class TestCheckModelVersionPinnedInRef:
    @pytest.mark.parametrize(
        ("refs", "check_fn"),
        [
            pytest.param(
                [{"name": "customers", "version": 2}],
                check_passes,
                id="downstream_ref_is_pinned",
            ),
            pytest.param(None, check_passes, id="refs_absent"),
            pytest.param([], check_passes, id="refs_empty"),
            pytest.param(
                [{"name": "orders", "version": None}],
                check_passes,
                id="refs_a_different_model",
            ),
            pytest.param(
                [{"version": None}],
                check_passes,
                id="ref_entry_has_no_name_key",
            ),
            pytest.param(
                [{"name": "Customers", "version": None}],
                check_passes,
                id="ref_name_case_mismatch",
            ),
            # Regression test: `unique_id` ends in the version (`v2`), while the
            # downstream `ref` names the model (`customers`). Comparing against
            # the last segment of `unique_id` made this check a silent no-op for
            # every genuinely versioned model.
            pytest.param(
                [{"name": "customers", "version": None}],
                check_fails,
                id="downstream_ref_is_unpinned",
            ),
            pytest.param(
                [{"name": "customers"}],
                check_fails,
                id="ref_has_no_version_key",
            ),
            # A single unpinned ref is enough to make the consumer a violator,
            # even where its other refs to the same model are correctly pinned.
            pytest.param(
                [
                    {"name": "customers", "version": 2},
                    {"name": "customers", "version": None},
                ],
                check_fails,
                id="only_some_of_a_consumers_refs_are_pinned",
            ),
        ],
    )
    def test_check_model_version_pinned_in_ref(self, refs, check_fn):
        check_fn(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map=_CHILD_MAP_V2,
                nodes={"model.package_name.orders": _downstream_model("orders", refs)},
            ),
        )

    @pytest.mark.parametrize(
        "refs",
        [
            pytest.param({"name": "customers"}, id="refs_is_a_dict"),
            pytest.param("customers", id="refs_is_a_string"),
        ],
    )
    def test_passes_when_refs_is_not_a_list(self, refs):
        # A truthy but non-list `refs` is guarded by an isinstance check rather
        # than being iterated.
        check_passes(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map=_CHILD_MAP_V2,
                nodes={
                    "model.package_name.orders": {
                        "name": "orders",
                        "refs": refs,
                        "unique_id": "model.package_name.orders",
                    },
                },
            ),
        )

    def test_passes_when_downstream_ref_is_pinned_to_version_zero(self):
        # A ref's version is compared against None, not truthiness, so
        # `ref('customers', v=0)` counts as pinned.
        check_passes(
            "check_model_version_pinned_in_ref",
            model={
                "latest_version": 0,
                "name": "customers",
                "unique_id": "model.package_name.customers.v0",
                "version": 0,
            },
            ctx_manifest_obj=_manifest(
                child_map={
                    "model.package_name.customers.v0": ["model.package_name.orders"],
                },
                nodes={
                    "model.package_name.orders": _downstream_model(
                        "orders", [{"name": "customers", "version": 0}]
                    ),
                },
            ),
        )

    def test_passes_for_an_unversioned_model(self):
        # Regression test: an unversioned model cannot be pinned, so downstream
        # refs without a version are correct rather than a violation.
        check_passes(
            "check_model_version_pinned_in_ref",
            model={
                "name": "stg_customers",
                "unique_id": "model.package_name.stg_customers",
                "version": None,
            },
            ctx_manifest_obj=_manifest(
                child_map={
                    "model.package_name.stg_customers": ["model.package_name.orders"],
                },
                nodes={
                    "model.package_name.orders": _downstream_model(
                        "orders", [{"name": "stg_customers", "version": None}]
                    ),
                },
            ),
        )

    @pytest.mark.parametrize(
        "child_map",
        [
            pytest.param(None, id="child_map_none"),
            pytest.param({}, id="child_map_empty"),
            pytest.param(
                {"model.package_name.unrelated": ["model.package_name.orders"]},
                id="model_absent_from_child_map",
            ),
        ],
    )
    def test_passes_when_model_has_no_entry_in_child_map(self, child_map):
        check_passes(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map=child_map,
                nodes={
                    "model.package_name.orders": _downstream_model(
                        "orders", [{"name": "customers", "version": None}]
                    ),
                },
            ),
        )

    @pytest.mark.parametrize(
        "downstream_unique_id",
        [
            pytest.param("test.package_name.unique_customers", id="test"),
            pytest.param("exposure.package_name.dashboard_1", id="exposure"),
            pytest.param("snapshot.package_name.snapshot_1", id="snapshot"),
            pytest.param("unit_test.package_name.customers.ut_1", id="unit_test"),
        ],
    )
    def test_passes_when_only_non_model_nodes_ref_unpinned(self, downstream_unique_id):
        # Only downstream nodes whose `unique_id` starts with `model.` are
        # considered; tests, exposures, snapshots and unit tests are ignored.
        check_passes(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map={"model.package_name.customers.v2": [downstream_unique_id]},
                nodes={
                    downstream_unique_id: {
                        "name": "downstream_1",
                        "refs": [{"name": "customers", "version": None}],
                        "unique_id": downstream_unique_id,
                    },
                },
            ),
        )

    def test_passes_when_downstream_node_missing_from_nodes(self):
        check_passes(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map=_CHILD_MAP_V2,
                nodes={},
            ),
        )

    def test_failure_message_includes_the_version_and_every_unpinned_consumer(self):
        check_fails(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map={
                    "model.package_name.customers.v2": [
                        "model.package_name.orders",
                        "model.package_name.payments",
                    ],
                },
                nodes={
                    "model.package_name.orders": _downstream_model(
                        "orders", [{"name": "customers", "version": None}]
                    ),
                    "model.package_name.payments": _downstream_model(
                        "payments", [{"name": "customers", "version": None}]
                    ),
                },
            ),
            match=(
                r"`customers_v2` is referenced without a pinned version in "
                r"downstream models: \["
                r"'model\.package_name\.orders', "
                r"'model\.package_name\.payments'\]\."
            ),
        )

    def test_failure_message_lists_only_the_unpinned_consumers(self):
        check_fails(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map={
                    "model.package_name.customers.v2": [
                        "model.package_name.orders",
                        "model.package_name.payments",
                    ],
                },
                nodes={
                    "model.package_name.orders": _downstream_model(
                        "orders", [{"name": "customers", "version": 2}]
                    ),
                    "model.package_name.payments": _downstream_model(
                        "payments", [{"name": "customers", "version": None}]
                    ),
                },
            ),
            match=r"downstream models: \['model\.package_name\.payments'\]\.",
        )

    def test_failure_message_lists_a_consumer_once_per_model_not_per_ref(self):
        # A downstream model that refs the same versioned model more than once
        # (e.g. a self-join across two versions) is a single violation.
        check_fails(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map=_CHILD_MAP_V2,
                nodes={
                    "model.package_name.orders": _downstream_model(
                        "orders",
                        [
                            {"name": "customers", "version": None},
                            {"name": "customers"},
                        ],
                    ),
                },
            ),
            match=r"downstream models: \['model\.package_name\.orders'\]\.",
        )
