from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.manifest.check_exposures import (
    check_exposure_based_on_non_public_models,
    check_exposure_based_on_view,
)


@pytest.mark.parametrize(
    "exposure, models, expectation",
    [
        (
            {
                "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                "name": "exposure_1",
                "unique_id": "exposure.dbt_bouncer_test_project.exposure_1",
            },
            [
                {
                    "access": "public",
                    "unique_id": "model.dbt_bouncer_test_project.model_1",
                }
            ],
            does_not_raise(),
        ),
        (
            {
                "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                "name": "exposure_1",
                "unique_id": "exposure.dbt_bouncer_test_project.exposure_1",
            },
            [
                {
                    "access": "protected",
                    "unique_id": "model.dbt_bouncer_test_project.model_1",
                }
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_exposure_based_on_non_public_models(exposure, models, expectation):
    with expectation:
        check_exposure_based_on_non_public_models(exposure=exposure, models=models, request=None)


@pytest.mark.parametrize(
    "check_config, exposure, models, expectation",
    [
        (
            {"materializations_to_include": ["ephemeral", "view"]},
            {
                "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                "name": "exposure_1",
                "unique_id": "exposure.dbt_bouncer_test_project.exposure_1",
            },
            [
                {
                    "config": {"materialized": "table"},
                    "unique_id": "model.dbt_bouncer_test_project.model_1",
                }
            ],
            does_not_raise(),
        ),
        (
            {"materializations_to_include": ["ephemeral", "view"]},
            {
                "depends_on": {
                    "nodes": [
                        "model.dbt_bouncer_test_project.model_1",
                        "model.dbt_bouncer_test_project.model_2",
                    ]
                },
                "name": "exposure_1",
                "unique_id": "exposure.dbt_bouncer_test_project.exposure_1",
            },
            [
                {
                    "config": {"materialized": "view"},
                    "unique_id": "model.dbt_bouncer_test_project.model_1",
                },
                {
                    "config": {"materialized": "table"},
                    "unique_id": "model.dbt_bouncer_test_project.model_2",
                },
            ],
            pytest.raises(AssertionError),
        ),
        (
            {"materializations_to_include": ["ephemeral", "view"]},
            {
                "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                "name": "exposure_1",
                "unique_id": "exposure.dbt_bouncer_test_project.exposure_1",
            },
            [
                {
                    "config": {"materialized": "view"},
                    "unique_id": "model.dbt_bouncer_test_project.model_1",
                }
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_exposure_based_on_view(check_config, exposure, models, expectation):
    with expectation:
        check_exposure_based_on_view(
            check_config=check_config, exposure=exposure, models=models, request=None
        )
