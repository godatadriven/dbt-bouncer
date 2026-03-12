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
