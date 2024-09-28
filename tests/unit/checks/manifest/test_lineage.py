import warnings
from contextlib import nullcontext as does_not_raise

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes4

from dbt_bouncer.artifact_parsers.parsers_manifest import (  # noqa: F401
    DbtBouncerManifest,
    DbtBouncerModelBase,
)
from dbt_bouncer.checks.manifest.check_lineage import (
    CheckLineagePermittedUpstreamModels,
    CheckLineageSeedCannotBeUsed,
    CheckLineageSourceCannotBeUsed,
)

CheckLineagePermittedUpstreamModels.model_rebuild()
CheckLineageSeedCannotBeUsed.model_rebuild()
CheckLineageSourceCannotBeUsed.model_rebuild()


@pytest.mark.parametrize(
    ("manifest_obj", "model", "models", "upstream_path_pattern", "expectation"),
    [
        (
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
                    "depends_on": {
                        "nodes": ["model.dbt_bouncer_test_project.stg_model_1"],
                    },
                    "fqn": ["dbt_bouncer_test_project", "int_model"],
                    "name": "int_model",
                    "original_file_path": "intermediate/int_model.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "intermediate/int_model.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.dbt_bouncer_test_project.int_model",
                },
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
                    },
                ),
            ],
            "^staging",
            does_not_raise(),
        ),
        (
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
                },
            ),
            [],
            "^staging",
            does_not_raise(),
        ),
        (
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
                    "depends_on": {
                        "nodes": ["model.dbt_bouncer_test_project.mart_other_model"],
                    },
                    "fqn": ["dbt_bouncer_test_project", "mart_model"],
                    "name": "mart_model",
                    "original_file_path": "marts/mart_model.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "marts/mart_model.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.dbt_bouncer_test_project.mart_model",
                },
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
                    },
                ),
            ],
            "^intermediate",
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_lineage_permitted_upstream_models(
    manifest_obj,
    model,
    models,
    upstream_path_pattern,
    expectation,
):
    with expectation:
        CheckLineagePermittedUpstreamModels(
            manifest_obj=manifest_obj,
            model=model,
            models=models,
            name="check_lineage_permitted_upstream_models",
            upstream_path_pattern=upstream_path_pattern,
        ).execute()


@pytest.mark.parametrize(
    ("model", "expectation"),
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
                },
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
                },
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_lineage_seed_cannot_be_used(model, expectation):
    with expectation:
        CheckLineageSeedCannotBeUsed(
            model=model,
            name="check_lineage_seed_cannot_be_used",
        ).execute()


@pytest.mark.parametrize(
    ("model", "expectation"),
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
                },
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
                },
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_lineage_source_cannot_be_used(model, expectation):
    with expectation:
        CheckLineageSourceCannotBeUsed(
            model=model,
            name="check_lineage_source_cannot_be_used",
        ).execute()
