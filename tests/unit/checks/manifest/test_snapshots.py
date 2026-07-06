import pytest

from dbt_bouncer.testing import check_fails, check_passes

_SNAPSHOT_BASE = {
    "alias": "snapshot_1",
    "config": {},
    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
    "name": "snapshot_1",
    "original_file_path": "snapshots/snapshot_1.sql",
    "path": "snapshot_1.sql",
    "unique_id": "snapshot.package_name.snapshot_1",
}


@pytest.mark.parametrize(
    ("snapshot_overrides", "min_description_length", "check_fn"),
    [
        pytest.param(
            {**_SNAPSHOT_BASE, "description": "A valid description."},
            None,
            check_passes,
            id="has_description",
        ),
        pytest.param(
            {
                **_SNAPSHOT_BASE,
                "description": "A valid description that is long enough.",
            },
            25,
            check_passes,
            id="has_description_min_length",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "description": ""},
            None,
            check_fails,
            id="empty_description",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "description": "abc"},
            None,
            check_fails,
            id="too_short_description",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "description": "Short desc."},
            25,
            check_fails,
            id="below_min_description_length",
        ),
    ],
)
def test_check_snapshot_description_populated(
    snapshot_overrides, min_description_length, check_fn
):
    kwargs = {"snapshot": snapshot_overrides}
    if min_description_length is not None:
        kwargs["min_description_length"] = min_description_length
    check_fn("check_snapshot_description_populated", **kwargs)


@pytest.mark.parametrize(
    ("snapshot_overrides", "keys", "check_fn"),
    [
        pytest.param(
            {**_SNAPSHOT_BASE, "meta": {"owner": "Data Team"}},
            ["owner"],
            check_passes,
            id="has_required_meta_key",
        ),
        pytest.param(
            {
                **_SNAPSHOT_BASE,
                "meta": {"owner": {"email": "team@example.com"}},
            },
            [{"owner": ["email"]}],
            check_passes,
            id="has_nested_key",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "meta": {}},
            ["owner"],
            check_fails,
            id="missing_required_meta_key",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "meta": {"owner": "team"}},
            ["owner", "maturity"],
            check_fails,
            id="missing_one_of_multiple_required_keys",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "meta": {"owner": {}}},
            [{"owner": ["email"]}],
            check_fails,
            id="missing_nested_key",
        ),
    ],
)
def test_check_snapshot_has_meta_keys(snapshot_overrides, keys, check_fn):
    check_fn("check_snapshot_has_meta_keys", keys=keys, snapshot=snapshot_overrides)


@pytest.mark.parametrize(
    ("snapshot_overrides", "tags", "criteria", "check_fn"),
    [
        pytest.param(
            {**_SNAPSHOT_BASE, "tags": ["tag_1"]},
            ["tag_1"],
            "all",
            check_passes,
            id="has_all_required_tags",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "tags": []},
            ["tag_1"],
            "all",
            check_fails,
            id="missing_all_tags",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "tags": ["tag_1"]},
            ["tag_1", "tag_2"],
            "any",
            check_passes,
            id="has_any_required_tag",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "tags": ["tag_3", "tag_4"]},
            ["tag_1", "tag_2"],
            "any",
            check_fails,
            id="missing_any_required_tag",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "tags": ["tag_1", "tag_3"]},
            ["tag_1", "tag_2"],
            "one",
            check_passes,
            id="has_exactly_one_required_tag",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "tags": ["tag_1", "tag_2"]},
            ["tag_1", "tag_2"],
            "one",
            check_fails,
            id="has_too_many_required_tags",
        ),
    ],
)
def test_check_snapshot_has_tags(snapshot_overrides, tags, criteria, check_fn):
    check_fn(
        "check_snapshot_has_tags",
        snapshot=snapshot_overrides,
        tags=tags,
        criteria=criteria,
    )


def test_check_snapshot_has_tags_invalid_criteria():
    with pytest.raises(ValueError, match="'any', 'all' or 'one'"):
        check_passes(
            "check_snapshot_has_tags",
            snapshot={**_SNAPSHOT_BASE, "tags": ["tag_1"]},
            tags=["tag_1"],
            criteria="alll",
        )


@pytest.mark.parametrize(
    ("snapshot_overrides", "check_fn"),
    [
        pytest.param(
            {**_SNAPSHOT_BASE, "config": {"unique_key": "id"}},
            check_passes,
            id="has_unique_key",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "config": {}},
            check_fails,
            id="missing_unique_key",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "config": {"unique_key": ""}},
            check_fails,
            id="empty_unique_key",
        ),
    ],
)
def test_check_snapshot_has_unique_key(snapshot_overrides, check_fn):
    check_fn("check_snapshot_has_unique_key", snapshot=snapshot_overrides)


@pytest.mark.parametrize(
    ("snapshot_overrides", "check_fn"),
    [
        pytest.param(
            {
                "alias": "snp_1",
                "config": {},
                "fqn": ["package_name", "snapshot_1", "snp_1"],
                "name": "snp_1",
                "original_file_path": "snapshots/snp_1.sql",
                "path": "snp_1.sql",
                "tags": ["tag_1"],
                "unique_id": "snapshot.package_name.snp_1",
            },
            check_passes,
            id="matches_pattern",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "tags": ["tag_1"]},
            check_fails,
            id="does_not_match_pattern",
        ),
    ],
)
def test_check_snapshot_names(snapshot_overrides, check_fn):
    check_fn(
        "check_snapshot_names",
        include="",
        snapshot_name_pattern="^snp_",
        snapshot=snapshot_overrides,
    )


@pytest.mark.parametrize(
    ("snapshot_overrides", "allowed_strategies", "check_fn"),
    [
        pytest.param(
            {
                **_SNAPSHOT_BASE,
                "config": {"strategy": "timestamp", "updated_at": "updated_at"},
            },
            ["check", "timestamp"],
            check_passes,
            id="timestamp_with_updated_at",
        ),
        pytest.param(
            {
                **_SNAPSHOT_BASE,
                "config": {"strategy": "check", "check_cols": ["col1", "col2"]},
            },
            ["check", "timestamp"],
            check_passes,
            id="check_with_check_cols",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "config": {"strategy": "timestamp"}},
            ["check", "timestamp"],
            check_fails,
            id="timestamp_without_updated_at",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "config": {"strategy": "check"}},
            ["check", "timestamp"],
            check_fails,
            id="check_without_check_cols",
        ),
        pytest.param(
            {**_SNAPSHOT_BASE, "config": {"strategy": "custom"}},
            ["check", "timestamp"],
            check_fails,
            id="unknown_strategy",
        ),
        pytest.param(
            {
                **_SNAPSHOT_BASE,
                "config": {"strategy": "timestamp", "updated_at": "updated_at"},
            },
            ["check"],
            check_fails,
            id="strategy_not_in_allowed_list",
        ),
    ],
)
def test_check_snapshot_strategy(snapshot_overrides, allowed_strategies, check_fn):
    check_fn(
        "check_snapshot_strategy",
        allowed_strategies=allowed_strategies,
        snapshot=snapshot_overrides,
    )
