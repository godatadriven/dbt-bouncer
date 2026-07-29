import re

import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelLatestVersionSpecified:
    @pytest.mark.parametrize(
        "model",
        [
            pytest.param({"latest_version": 2}, id="latest_version_integer"),
            pytest.param({"latest_version": "stable"}, id="latest_version_string"),
            # `latest_version` is compared against None, so a falsy-but-set value
            # is still a specified version.
            pytest.param({"latest_version": 0}, id="latest_version_zero"),
        ],
    )
    def test_passes(self, model):
        check_passes("check_model_latest_version_specified", model=model)

    @pytest.mark.parametrize(
        "model",
        [
            pytest.param({"latest_version": None}, id="latest_version_none"),
            pytest.param({}, id="latest_version_absent"),
        ],
    )
    def test_fails(self, model):
        check_fails("check_model_latest_version_specified", model=model)

    def test_failure_message_names_the_model(self):
        check_fails(
            "check_model_latest_version_specified",
            model={"latest_version": None, "name": "customers"},
            match=r"`customers` does not have a specified `latest_version`",
        )


class TestCheckModelVersionAllowed:
    @pytest.mark.parametrize(
        ("version", "version_pattern"),
        [
            pytest.param(1, r"[0-9]\d*", id="integer_version_1"),
            pytest.param(10, r"[0-9]\d*", id="integer_version_10"),
            pytest.param(100, r"[0-9]\d*", id="integer_version_100"),
            pytest.param("stable", r"^(stable|latest)$", id="string_version_stable"),
            pytest.param("latest", r"^(stable|latest)$", id="string_version_latest"),
            # An unversioned model has nothing to validate, so the pattern is
            # never applied.
            pytest.param(None, r"^never_matches$", id="unversioned_model_skipped"),
            # `version` is truthiness-checked, so a version of 0 is also skipped.
            pytest.param(0, r"^never_matches$", id="falsy_version_zero_skipped"),
        ],
    )
    def test_passes(self, version, version_pattern):
        check_passes(
            "check_model_version_allowed",
            model={"version": version},
            version_pattern=version_pattern,
        )

    @pytest.mark.parametrize(
        ("version", "version_pattern"),
        [
            pytest.param("golden", r"[0-9]\d*", id="non_numeric_version"),
            pytest.param(2, r"^(stable|latest)$", id="numeric_version_not_allowed"),
        ],
    )
    def test_fails(self, version, version_pattern):
        check_fails(
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
    def test_passes_when_downstream_ref_is_pinned(self):
        check_passes(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map={
                    "model.package_name.customers.v2": ["model.package_name.orders"],
                },
                nodes={
                    "model.package_name.orders": _downstream_model(
                        "orders", [{"name": "customers", "version": 2}]
                    ),
                },
            ),
        )

    def test_fails_when_downstream_ref_is_unpinned(self):
        # Regression test: `unique_id` ends in the version (`v2`), while the
        # downstream `ref` names the model (`customers`). Comparing against the
        # last segment of `unique_id` made this check a silent no-op for every
        # genuinely versioned model.
        check_fails(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map={
                    "model.package_name.customers.v2": ["model.package_name.orders"],
                },
                nodes={
                    "model.package_name.orders": _downstream_model(
                        "orders", [{"name": "customers", "version": None}]
                    ),
                },
            ),
        )

    def test_fails_when_ref_has_no_version_key(self):
        check_fails(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map={
                    "model.package_name.customers.v2": ["model.package_name.orders"],
                },
                nodes={
                    "model.package_name.orders": _downstream_model(
                        "orders", [{"name": "customers"}]
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
                child_map={
                    "model.package_name.customers.v2": ["model.package_name.orders"],
                },
                nodes={},
            ),
        )

    @pytest.mark.parametrize(
        "refs",
        [
            pytest.param(None, id="refs_absent"),
            pytest.param([], id="refs_empty"),
            pytest.param(
                [{"name": "orders", "version": None}], id="refs_a_different_model"
            ),
        ],
    )
    def test_passes_when_downstream_does_not_ref_this_model(self, refs):
        check_passes(
            "check_model_version_pinned_in_ref",
            model=_MODEL_V2,
            ctx_manifest_obj=_manifest(
                child_map={
                    "model.package_name.customers.v2": ["model.package_name.orders"],
                },
                nodes={"model.package_name.orders": _downstream_model("orders", refs)},
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
