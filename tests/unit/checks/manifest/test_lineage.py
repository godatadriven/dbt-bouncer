from contextlib import nullcontext as does_not_raise

import pytest
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes4

from dbt_bouncer.checks.manifest.check_lineage import (
    check_lineage_permitted_upstream_models,
    check_lineage_seed_cannot_be_used,
    check_lineage_source_cannot_be_used,
)


@pytest.mark.parametrize(
    "check_config, manifest_obj, model, models, expectation",
    [
        (
            {
                "upstream_path_pattern": "^staging",
            },
            "manifest_obj",
            Nodes4(
                **{
                    "alias": "int_model",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["model.dbt_bouncer_test_project.stg_model_1"]},
                    "fqn": ["dbt_bouncer_test_project", "int_model"],
                    "name": "int_model",
                    "original_file_path": "intermediate/int_model.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "intermediate/int_model.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.dbt_bouncer_test_project.int_model",
                }
            ),
            [
                Nodes4(
                    **{
                        "alias": "stg_model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["dbt_bouncer_test_project", "stg_model_1"],
                        "name": "stg_model_1",
                        "original_file_path": "staging/stg_model_1.sql",
                        "package_name": "dbt_bouncer_test_project",
                        "path": "staging/stg_model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.dbt_bouncer_test_project.stg_model_1",
                    }
                )
            ],
            does_not_raise(),
        ),
        (
            {
                "upstream_path_pattern": "^staging",
            },
            "manifest_obj",
            Nodes4(
                **{
                    "alias": "int_model",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["model.some_other_package.stg_model"]},
                    "fqn": ["dbt_bouncer_test_project", "int_model"],
                    "name": "int_model",
                    "original_file_path": "intermediate/int_model.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "intermediate/int_model.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.dbt_bouncer_test_project.int_model",
                }
            ),
            [],
            does_not_raise(),
        ),
        (
            {
                "upstream_path_pattern": "^intermediate",
            },
            "manifest_obj",
            Nodes4(
                **{
                    "alias": "mart_model",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["model.dbt_bouncer_test_project.mart_other_model"]},
                    "fqn": ["dbt_bouncer_test_project", "mart_model"],
                    "name": "mart_model",
                    "original_file_path": "marts/mart_model.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "marts/mart_model.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.dbt_bouncer_test_project.mart_model",
                }
            ),
            [
                Nodes4(
                    **{
                        "alias": "mart_other_model",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["dbt_bouncer_test_project", "mart_other_model"],
                        "name": "mart_other_model",
                        "original_file_path": "marts/mart_other_model.sql",
                        "package_name": "dbt_bouncer_test_project",
                        "path": "marts/mart_other_model.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.dbt_bouncer_test_project.mart_other_model",
                    }
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_lineage_permitted_upstream_models(
    check_config, manifest_obj, model, models, expectation
):
    with expectation:
        check_lineage_permitted_upstream_models(
            check_config=check_config,
            manifest_obj=manifest_obj,
            model=model,
            models=models,
            request=None,
        )


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            Nodes4(
                **{
                    "alias": "int_model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
                    "fqn": ["package_name", "int_model_2"],
                    "name": "int_model_2",
                    "original_file_path": "intermediate/int_model_2.sql",
                    "package_name": "package_name",
                    "path": "intermediate/int_model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.int_model_2",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
                    "alias": "int_model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["seed.package_name.seed_1"]},
                    "fqn": ["package_name", "int_model_2"],
                    "name": "int_model_2",
                    "original_file_path": "intermediate/int_model_2.sql",
                    "package_name": "package_name",
                    "path": "intermediate/int_model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.int_model_2",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_lineage_seed_cannot_be_used(model, expectation):
    with expectation:
        check_lineage_seed_cannot_be_used(model=model, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            Nodes4(
                **{
                    "alias": "int_model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
                    "fqn": ["package_name", "int_model_2"],
                    "name": "int_model_2",
                    "original_file_path": "intermediate/int_model_2.sql",
                    "package_name": "package_name",
                    "path": "intermediate/int_model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.int_model_2",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
                    "alias": "int_model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                    "fqn": ["package_name", "int_model_2"],
                    "name": "int_model_2",
                    "original_file_path": "intermediate/int_model_2.sql",
                    "package_name": "package_name",
                    "path": "intermediate/int_model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.int_model_2",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_lineage_source_cannot_be_used(model, expectation):
    with expectation:
        check_lineage_source_cannot_be_used(model=model, request=None)
