from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    ManifestLatest,
    Metadata,
)
from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Nodes as SeedsLatest,
)
from dbt_bouncer.artifact_parsers.parsers_catalog import CatalogNodes
from dbt_bouncer.artifact_parsers.parsers_manifest import DbtBouncerManifest
from dbt_bouncer.checks.catalog.check_catalog_seeds import (
    CheckSeedColumnsAreAllDocumented,
)
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


@pytest.mark.parametrize(
    ("catalog_node", "seeds", "expectation"),
    [
        pytest.param(
            CatalogNodes(
                **{
                    "columns": {
                        "id": {"name": "id", "type": "INTEGER", "index": 1},
                        "first_name": {
                            "name": "first_name",
                            "type": "VARCHAR",
                            "index": 2,
                        },
                        "last_name": {
                            "name": "last_name",
                            "type": "VARCHAR",
                            "index": 3,
                        },
                    },
                    "metadata": {
                        "database": "dbt",
                        "name": "raw_customers",
                        "schema": "main",
                        "type": "BASE TABLE",
                    },
                    "stats": {},
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            [
                SeedsLatest(
                    **{
                        "alias": "raw_customers",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "id": {"name": "id"},
                            "first_name": {"name": "first_name"},
                            "last_name": {"name": "last_name"},
                        },
                        "fqn": ["package_name", "raw_customers"],
                        "name": "raw_customers",
                        "original_file_path": "seeds/raw_customers.csv",
                        "package_name": "package_name",
                        "path": "raw_customers.csv",
                        "resource_type": "seed",
                        "schema": "main",
                        "unique_id": "seed.package_name.raw_customers",
                    }
                )
            ],
            does_not_raise(),
            id="all_columns_documented",
        ),
        pytest.param(
            CatalogNodes(
                **{
                    "columns": {
                        "id": {"name": "id", "type": "INTEGER", "index": 1},
                        "first_name": {
                            "name": "first_name",
                            "type": "VARCHAR",
                            "index": 2,
                        },
                        "last_name": {
                            "name": "last_name",
                            "type": "VARCHAR",
                            "index": 3,
                        },
                    },
                    "metadata": {
                        "database": "dbt",
                        "name": "raw_customers",
                        "schema": "main",
                        "type": "BASE TABLE",
                    },
                    "stats": {},
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            [
                SeedsLatest(
                    **{
                        "alias": "raw_customers",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "id": {"name": "id"},
                            "first_name": {"name": "first_name"},
                        },
                        "fqn": ["package_name", "raw_customers"],
                        "name": "raw_customers",
                        "original_file_path": "seeds/raw_customers.csv",
                        "package_name": "package_name",
                        "path": "raw_customers.csv",
                        "resource_type": "seed",
                        "schema": "main",
                        "unique_id": "seed.package_name.raw_customers",
                    }
                )
            ],
            pytest.raises(DbtBouncerFailedCheckError),
            id="missing_last_name_column",
        ),
    ],
)
def test_check_seed_columns_are_all_documented(catalog_node, seeds, expectation):
    with expectation:
        CheckSeedColumnsAreAllDocumented(
            catalog_node=catalog_node,
            manifest_obj=DbtBouncerManifest(
                manifest=ManifestLatest(
                    **{
                        "metadata": Metadata(
                            dbt_schema_version="https://schemas.getdbt.com/dbt/manifest/v12.json",
                            dbt_version="1.8.0",
                            generated_at=None,
                            invocation_id=None,
                            invocation_started_at=None,
                            env=None,
                            project_name="dbt_bouncer_test_project",
                            project_id=None,
                            user_id=None,
                            send_anonymous_usage_stats=None,
                            adapter_type="postgres",
                            quoting=None,
                            run_started_at=None,
                        ),
                        "nodes": {},
                        "sources": {},
                        "macros": {},
                        "docs": {},
                        "exposures": {},
                        "metrics": {},
                        "groups": {},
                        "selectors": {},
                        "disabled": {},
                        "parent_map": {},
                        "child_map": {},
                        "group_map": {},
                        "saved_queries": {},
                        "semantic_models": {},
                        "unit_tests": {},
                    }
                ),
            ),
            name="check_seed_columns_are_all_documented",
            seeds=seeds,
        ).execute()
