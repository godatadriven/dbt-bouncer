from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    ManifestLatest,
    Metadata,
    UnitTests,
)
from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Nodes as SeedsLatest,
)
from dbt_bouncer.artifact_parsers.parsers_manifest import DbtBouncerManifest
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.check_seeds import (
    CheckSeedColumnNames,
    CheckSeedColumnsHaveTypes,
    CheckSeedDescriptionPopulated,
    CheckSeedHasUnitTests,
    CheckSeedNames,
)


@pytest.mark.parametrize(
    ("seed", "seed_column_name_pattern", "expectation"),
    [
        pytest.param(
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
            ),
            "^[a-z_]+$",
            does_not_raise(),
            id="all_columns_match_pattern",
        ),
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "id": {"name": "id"},
                        "firstName": {"name": "firstName"},
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
            ),
            "^[a-z_]+$",
            pytest.raises(DbtBouncerFailedCheckError),
            id="camelCase_column_name",
        ),
    ],
)
def test_check_seed_column_names(seed, seed_column_name_pattern, expectation):
    with expectation:
        CheckSeedColumnNames(
            seed=seed,
            seed_column_name_pattern=seed_column_name_pattern,
            name="check_seed_column_names",
        ).execute()


@pytest.mark.parametrize(
    ("seed", "expectation"),
    [
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "id": {"name": "id", "data_type": "integer"},
                        "first_name": {"name": "first_name", "data_type": "varchar"},
                        "last_name": {"name": "last_name", "data_type": "varchar"},
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
            ),
            does_not_raise(),
            id="all_columns_have_types",
        ),
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "id": {"name": "id", "data_type": "integer"},
                        "first_name": {"name": "first_name"},
                        "last_name": {"name": "last_name", "data_type": "varchar"},
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
            ),
            pytest.raises(DbtBouncerFailedCheckError),
            id="missing_data_type",
        ),
    ],
)
def test_check_seed_columns_have_types(seed, expectation):
    with expectation:
        CheckSeedColumnsHaveTypes(
            seed=seed,
            name="check_seed_columns_have_types",
        ).execute()


@pytest.mark.parametrize(
    ("seed", "expectation"),
    [
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "package_name": "package_name",
                    "path": "raw_customers.csv",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            does_not_raise(),
        ),
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "description": """A
                            multiline
                            description
                            """,
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "package_name": "package_name",
                    "path": "raw_customers.csv",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            does_not_raise(),
        ),
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "description": "",
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "package_name": "package_name",
                    "path": "raw_customers.csv",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            pytest.raises(DbtBouncerFailedCheckError),
        ),
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "description": " ",
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "package_name": "package_name",
                    "path": "raw_customers.csv",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            pytest.raises(DbtBouncerFailedCheckError),
        ),
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "description": """
                            """,
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "package_name": "package_name",
                    "path": "raw_customers.csv",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            pytest.raises(DbtBouncerFailedCheckError),
        ),
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "description": "-",
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "package_name": "package_name",
                    "path": "raw_customers.csv",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            pytest.raises(DbtBouncerFailedCheckError),
        ),
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "description": "null",
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "package_name": "package_name",
                    "path": "raw_customers.csv",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            pytest.raises(DbtBouncerFailedCheckError),
        ),
    ],
)
def test_check_seed_description_populated(seed, expectation):
    with expectation:
        CheckSeedDescriptionPopulated(
            name="check_seed_description_populated",
            seed=seed,
        ).execute()


@pytest.mark.parametrize(
    ("manifest_obj", "min_number_of_unit_tests", "seed", "unit_tests", "expectation"),
    [
        pytest.param(
            DbtBouncerManifest(
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
            1,
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "package_name": "package_name",
                    "path": "raw_customers.csv",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            [
                UnitTests(
                    **{
                        "depends_on": {
                            "nodes": [
                                "seed.package_name.raw_customers",
                            ],
                        },
                        "expect": {"format": "dict", "rows": [{"id": 1}]},
                        "fqn": [
                            "package_name",
                            "raw_customers",
                            "unit_test_1",
                        ],
                        "given": [{"input": "ref(input_1)", "format": "csv"}],
                        "model": "raw_customers",
                        "name": "unit_test_1",
                        "original_file_path": "seeds/_seeds.yml",
                        "resource_type": "unit_test",
                        "package_name": "package_name",
                        "path": "_seeds.yml",
                        "unique_id": "unit_test.package_name.raw_customers.unit_test_1",
                    }
                ),
            ],
            does_not_raise(),
            id="has_unit_test",
        ),
        pytest.param(
            DbtBouncerManifest(
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
            2,
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "package_name": "package_name",
                    "path": "raw_customers.csv",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            [
                UnitTests(
                    **{
                        "depends_on": {
                            "nodes": [
                                "seed.package_name.raw_customers",
                            ],
                        },
                        "expect": {"format": "dict", "rows": [{"id": 1}]},
                        "fqn": [
                            "package_name",
                            "raw_customers",
                            "unit_test_1",
                        ],
                        "given": [{"input": "ref(input_1)", "format": "csv"}],
                        "model": "raw_customers",
                        "name": "unit_test_1",
                        "original_file_path": "seeds/_seeds.yml",
                        "resource_type": "unit_test",
                        "package_name": "package_name",
                        "path": "_seeds.yml",
                        "unique_id": "unit_test.package_name.raw_customers.unit_test_1",
                    }
                ),
            ],
            pytest.raises(DbtBouncerFailedCheckError),
            id="not_enough_unit_tests",
        ),
    ],
)
def test_check_seed_has_unit_tests(
    manifest_obj,
    min_number_of_unit_tests,
    seed,
    unit_tests,
    expectation,
):
    with expectation:
        CheckSeedHasUnitTests(
            manifest_obj=manifest_obj,
            min_number_of_unit_tests=min_number_of_unit_tests,
            seed=seed,
            name="check_seed_has_unit_tests",
            unit_tests=unit_tests,
        ).execute()


@pytest.mark.parametrize(
    ("seed", "seed_name_pattern", "expectation"),
    [
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.sql",
                    "package_name": "package_name",
                    "path": "raw_customers.sql",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            "^raw_",
            does_not_raise(),
        ),
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.sql",
                    "package_name": "package_name",
                    "path": "raw_customers.sql",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            "^seed_",
            pytest.raises(DbtBouncerFailedCheckError),
        ),
    ],
)
def test_check_seed_names(seed, seed_name_pattern, expectation):
    with expectation:
        CheckSeedNames(
            seed=seed, seed_name_pattern=seed_name_pattern, name="check_seed_names"
        ).execute()
