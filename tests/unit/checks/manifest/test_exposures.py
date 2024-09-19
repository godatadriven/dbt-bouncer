import warnings
from contextlib import nullcontext as does_not_raise

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Exposures

from dbt_bouncer.checks.manifest.check_exposures import (
    CheckExposureOnNonPublicModels,
    CheckExposureOnView,
)
from dbt_bouncer.parsers import (  # noqa: F401
    DbtBouncerExposureBase,
    DbtBouncerModel,
    DbtBouncerModelBase,
    DbtBouncerNodes4,
)

CheckExposureOnNonPublicModels.model_rebuild()
CheckExposureOnView.model_rebuild()


@pytest.mark.parametrize(
    ("exposure", "models", "expectation"),
    [
        (
            Exposures(
                **{
                    "depends_on": {"nodes": ["model.package_name.model_1"]},
                    "fqn": ["package_name", "marts", "finance", "exposure_1"],
                    "name": "exposure_1",
                    "original_file_path": "models/marts/finance/_exposures.yml",
                    "owner": {
                        "email": "anna.anderson@example.com",
                        "name": "Anna Anderson",
                    },
                    "package_name": "package_name",
                    "path": "marts/finance/_exposures.yml",
                    "resource_type": "exposure",
                    "type": "dashboard",
                    "unique_id": "exposure.package_name.exposure_1",
                },
            ),
            [
                DbtBouncerNodes4(
                    **{
                        "access": "public",
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                            "col_2": {
                                "index": 2,
                                "name": "col_2",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    },
                ),
            ],
            does_not_raise(),
        ),
        (
            Exposures(
                **{
                    "depends_on": {"nodes": ["model.package_name.model_1"]},
                    "fqn": ["package_name", "marts", "finance", "exposure_1"],
                    "name": "exposure_1",
                    "original_file_path": "models/marts/finance/_exposures.yml",
                    "owner": {
                        "email": "anna.anderson@example.com",
                        "name": "Anna Anderson",
                    },
                    "package_name": "package_name",
                    "path": "marts/finance/_exposures.yml",
                    "resource_type": "exposure",
                    "type": "dashboard",
                    "unique_id": "exposure.package_name.exposure_1",
                },
            ),
            [
                DbtBouncerNodes4(
                    **{
                        "access": "protected",
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                            "col_2": {
                                "index": 2,
                                "name": "col_2",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    },
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_exposure_based_on_non_public_models(exposure, models, expectation):
    with expectation:
        CheckExposureOnNonPublicModels(
            exposure=exposure,
            models=models,
            name="check_exposure_based_on_non_public_models",
        ).execute()


@pytest.mark.parametrize(
    ("exposure", "materializations_to_include", "models", "expectation"),
    [
        (
            Exposures(
                **{
                    "depends_on": {"nodes": ["model.package_name.model_1"]},
                    "fqn": ["package_name", "marts", "finance", "exposure_1"],
                    "name": "exposure_1",
                    "original_file_path": "models/marts/finance/_exposures.yml",
                    "owner": {
                        "email": "anna.anderson@example.com",
                        "name": "Anna Anderson",
                    },
                    "package_name": "package_name",
                    "path": "marts/finance/_exposures.yml",
                    "resource_type": "exposure",
                    "type": "dashboard",
                    "unique_id": "exposure.package_name.exposure_1",
                },
            ),
            ["ephemeral", "view"],
            [
                DbtBouncerNodes4(
                    **{
                        "access": "protected",
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "config": {"materialized": "table"},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                            "col_2": {
                                "index": 2,
                                "name": "col_2",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    },
                ),
            ],
            does_not_raise(),
        ),
        (
            Exposures(
                **{
                    "depends_on": {
                        "nodes": [
                            "model.package_name.model_1",
                            "model.package_name.model_2",
                        ],
                    },
                    "fqn": ["package_name", "marts", "finance", "exposure_1"],
                    "name": "exposure_1",
                    "original_file_path": "models/marts/finance/_exposures.yml",
                    "owner": {
                        "email": "anna.anderson@example.com",
                        "name": "Anna Anderson",
                    },
                    "package_name": "package_name",
                    "path": "marts/finance/_exposures.yml",
                    "resource_type": "exposure",
                    "type": "dashboard",
                    "unique_id": "exposure.package_name.exposure_1",
                },
            ),
            ["ephemeral", "view"],
            [
                DbtBouncerNodes4(
                    **{
                        "access": "protected",
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "config": {"materialized": "view"},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                            "col_2": {
                                "index": 2,
                                "name": "col_2",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    },
                ),
                DbtBouncerNodes4(
                    **{
                        "access": "protected",
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "config": {"materialized": "table"},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                            "col_2": {
                                "index": 2,
                                "name": "col_2",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "model_2.sql",
                        "package_name": "package_name",
                        "path": "model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    },
                ),
            ],
            pytest.raises(AssertionError),
        ),
        (
            Exposures(
                **{
                    "depends_on": {"nodes": ["model.package_name.model_1"]},
                    "fqn": ["package_name", "marts", "finance", "exposure_1"],
                    "name": "exposure_1",
                    "original_file_path": "models/marts/finance/_exposures.yml",
                    "owner": {
                        "email": "anna.anderson@example.com",
                        "name": "Anna Anderson",
                    },
                    "package_name": "package_name",
                    "path": "marts/finance/_exposures.yml",
                    "resource_type": "exposure",
                    "type": "dashboard",
                    "unique_id": "exposure.package_name.exposure_1",
                },
            ),
            ["ephemeral", "view"],
            [
                DbtBouncerNodes4(
                    **{
                        "access": "protected",
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "config": {"materialized": "view"},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                            "col_2": {
                                "index": 2,
                                "name": "col_2",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    },
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_exposure_based_on_view(
    exposure,
    materializations_to_include,
    models,
    expectation,
):
    with expectation:
        CheckExposureOnView(
            exposure=exposure,
            materializations_to_include=materializations_to_include,
            models=models,
            name="check_exposure_based_on_view",
        ).execute()
