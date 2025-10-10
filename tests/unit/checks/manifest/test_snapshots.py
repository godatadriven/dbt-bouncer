import warnings
from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import Nodes7

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)

    from dbt_bouncer.artifact_parsers.parsers_manifest import (  # noqa: F401
        DbtBouncerManifest,
        DbtBouncerSnapshot,
        DbtBouncerSnapshotBase,
    )

from dbt_bouncer.checks.manifest.check_snapshots import (
    CheckSnapshotHasTags,
    CheckSnapshotNames,
)

CheckSnapshotHasTags.model_rebuild()
CheckSnapshotNames.model_rebuild()


@pytest.mark.parametrize(
    ("snapshot", "tags", "expectation"),
    [
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": ["tag_1"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1"],
            does_not_raise(),
        ),
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": [],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_2"],
            pytest.raises(AssertionError),
        ),
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": [],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1"],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_snapshot_has_tags_default(snapshot, tags, expectation):
    """Default criteria is 'all'."""
    with expectation:
        CheckSnapshotHasTags(
            snapshot=snapshot,
            name="check_snapshot_has_tags",
            tags=tags,
        ).execute()


@pytest.mark.parametrize(
    ("snapshot", "tags", "expectation"),
    [
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": ["tag_1"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1"],
            does_not_raise(),
        ),
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": [],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1"],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_snapshot_has_tags_all(snapshot, tags, expectation):
    """Criteria is 'all'."""
    with expectation:
        CheckSnapshotHasTags(
            snapshot=snapshot,
            name="check_snapshot_has_tags",
            tags=tags,
            criteria="all",
        ).execute()


@pytest.mark.parametrize(
    ("snapshot", "tags", "expectation"),
    [
        # Snapshot has one of the required tags - should pass
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": ["tag_1"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            does_not_raise(),
        ),
        # Snapshot has multiple tags including one of the required - should pass
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": ["tag_1", "tag_3"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            does_not_raise(),
        ),
        # Snapshot has no tags - should fail
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": [],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            pytest.raises(AssertionError),
        ),
        # Snapshot has tags but none of the required ones - should fail
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": ["tag_3", "tag_4"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_snapshot_has_tags_any(snapshot, tags, expectation):
    """Criteria is 'any'."""
    with expectation:
        CheckSnapshotHasTags(
            snapshot=snapshot,
            name="check_snapshot_has_tags",
            tags=tags,
            criteria="any",
        ).execute()


@pytest.mark.parametrize(
    ("snapshot", "tags", "expectation"),
    [
        # Snapshot has exactly one of the required tags - should pass
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": ["tag_1", "tag_3"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            does_not_raise(),
        ),
        # Snapshot has multiple required tags - should fail
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": ["tag_1", "tag_2"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            pytest.raises(AssertionError),
        ),
        # Snapshot has no required tags - should fail
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": [],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            pytest.raises(AssertionError),
        ),
        # Snapshot has no tags but only one required - should fail
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": [],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1"],
            pytest.raises(AssertionError),
        ),
        # Snapshot has exactly one required tag from single tag list - should pass
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": ["tag_1"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1"],
            does_not_raise(),
        ),
        # Snapshot has tags but none of the required ones - should fail
        (
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": ["tag_3", "tag_4"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_snapshot_has_tags_one(snapshot, tags, expectation):
    """Criteria is 'one'."""
    with expectation:
        CheckSnapshotHasTags(
            snapshot=snapshot,
            name="check_snapshot_has_tags",
            tags=tags,
            criteria="one",
        ).execute()


@pytest.mark.parametrize(
    ("include", "snapshot_name_pattern", "snapshot", "expectation"),
    [
        (
            "",
            "^snp_",
            Nodes7(
                **{
                    "alias": "snp_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snp_1"],
                    "name": "snp_1",
                    "original_file_path": "snapshots/snp_1.sql",
                    "package_name": "package_name",
                    "path": "snp_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": ["tag_1"],
                    "unique_id": "snapshot.package_name.snp_1",
                },
            ),
            does_not_raise(),
        ),
        (
            "",
            "^snp_",
            Nodes7(
                **{
                    "alias": "snapshot_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {},
                    "fqn": ["package_name", "snapshot_1", "snapshot_1"],
                    "name": "snapshot_1",
                    "original_file_path": "snapshots/snapshot_1.sql",
                    "package_name": "package_name",
                    "path": "snapshot_1.sql",
                    "resource_type": "snapshot",
                    "schema": "main",
                    "tags": ["tag_1"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_snapshot_names(include, snapshot_name_pattern, snapshot, expectation):
    with expectation:
        CheckSnapshotNames(
            include=include,
            snapshot_name_pattern=snapshot_name_pattern,
            snapshot=snapshot,
            name="check_snapshot_names",
        ).execute()
