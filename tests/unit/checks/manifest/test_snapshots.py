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
    ("snapshot", "tags", "criteria", "expectation"),
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
            "all",
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
            "all",
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
                    "tags": ["tag_1"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            "any",
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
                    "tags": ["tag_3", "tag_4"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            "any",
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
                    "tags": ["tag_1", "tag_3"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            "one",
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
                    "tags": ["tag_1", "tag_2"],
                    "unique_id": "snapshot.package_name.snapshot_1",
                },
            ),
            ["tag_1", "tag_2"],
            "one",
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_snapshot_has_tags(snapshot, tags, criteria, expectation):
    with expectation:
        CheckSnapshotHasTags(
            snapshot=snapshot,
            name="check_snapshot_has_tags",
            tags=tags,
            criteria=criteria,
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
