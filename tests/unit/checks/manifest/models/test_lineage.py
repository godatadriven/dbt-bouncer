from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.models.lineage import (
    CheckModelDependsOnMacros,
    CheckModelDependsOnMultipleSources,
    CheckModelHasExposure,
    CheckModelHasNoUpstreamDependencies,
    CheckModelMaxChainedViews,
    CheckModelMaxFanout,
    CheckModelMaxUpstreamDependencies,
)

_TEST_DATA_FOR_CHECK_MODEL_DEPENDS_ON_MACROS = [
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {"macros": ["macro.dbt.is_incremental"]},
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.is_incremental"],
        "all",
        does_not_raise(),
        id="depends_on_required_macro",
    ),
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {"macros": ["macro.dbt.is_incremental"]},
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.is_incremental", "dbt.other_macro"],
        "all",
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_one_required_macro",
    ),
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {
                "macros": [
                    "macro.dbt.is_incremental",
                    "macro.dbt.other_macro",
                ]
            },
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.is_incremental"],
        "any",
        does_not_raise(),
        id="depends_on_any_macro",
    ),
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {"macros": ["macro.dbt.is_incremental"]},
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.other_macro"],
        "any",
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_any_required_macro",
    ),
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {"macros": ["macro.dbt.is_incremental"]},
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.is_incremental", "dbt.other_macro"],
        "one",
        does_not_raise(),
        id="depends_on_one_macro",
    ),
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {
                "macros": [
                    "macro.dbt.is_incremental",
                    "macro.dbt.other_macro",
                ]
            },
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.is_incremental", "dbt.other_macro"],
        "one",
        pytest.raises(DbtBouncerFailedCheckError),
        id="depends_on_too_many_macros",
    ),
]


@pytest.mark.parametrize(
    ("model", "required_macros", "criteria", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DEPENDS_ON_MACROS,
    indirect=["model"],
)
def test_check_model_depends_on_macros(model, required_macros, criteria, expectation):
    with expectation:
        CheckModelDependsOnMacros(
            model=model,
            required_macros=required_macros,
            criteria=criteria,
            name="check_model_depends_on_macros",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_DEPENDS_ON_MULTIPLE_SOURCES = [
    pytest.param(
        {
            "alias": "model_2",
            "depends_on": {"nodes": ["source.package_name.source_1"]},
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        does_not_raise(),
        id="single_source_dependency",
    ),
    pytest.param(
        {
            "alias": "model_2",
            "depends_on": {
                "nodes": [
                    "source.package_name.source_1",
                    "source.package_name.source_2",
                ],
            },
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="multiple_source_dependency",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DEPENDS_ON_MULTIPLE_SOURCES,
    indirect=["model"],
)
def test_check_model_depends_on_multiple_sources(model, expectation):
    with expectation:
        CheckModelDependsOnMultipleSources(
            model=model, name="check_model_depends_on_multiple_sources"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_EXPOSURES = [
    pytest.param(
        [
            {},  # default exposure depends on model_1
        ],
        {
            "depends_on": {"nodes": ["source.package_name.source_1"]},
        },
        does_not_raise(),
        id="has_exposure",
    ),
    pytest.param(
        [
            {
                "depends_on": {"nodes": ["model.package_name.model_2"]},
            },
        ],
        {
            "depends_on": {"nodes": ["source.package_name.source_1"]},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="no_exposure",
    ),
]


@pytest.mark.parametrize(
    ("exposures", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_EXPOSURES,
    indirect=["exposures", "model"],
)
def test_check_model_has_exposures(exposures, model, expectation):
    with expectation:
        CheckModelHasExposure(
            exposures=exposures, model=model, name="check_model_has_exposure"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_NO_UPSTREAM_DEPENDENCIES = [
    pytest.param(
        {
            "depends_on": {"nodes": ["source.package_name.source_1"]},
        },
        does_not_raise(),
        id="depends_on_source",
    ),
    pytest.param(
        {
            "alias": "int_model_1",
            "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
            "fqn": ["package_name", "int_model_1"],
            "name": "int_model_1",
            "original_file_path": "models/int_model_1.sql",
            "path": "int_model_1.sql",
            "unique_id": "model.package_name.int_model_1",
        },
        does_not_raise(),
        id="depends_on_model",
    ),
    pytest.param(
        {
            "depends_on": {"nodes": []},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="no_dependencies",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_NO_UPSTREAM_DEPENDENCIES,
    indirect=["model"],
)
def test_check_model_has_no_upstream_dependencies(model, expectation):
    with expectation:
        CheckModelHasNoUpstreamDependencies(
            model=model, name="check_model_has_no_upstream_dependencies"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_MAX_CHAINED_VIEWS = [
    pytest.param(
        "manifest_obj",
        ["ephemeral", "view"],
        3,
        {
            "alias": "model_0",
            "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
            "fqn": ["dbt_bouncer_test_project", "model_1"],
            "name": "model_0",
            "original_file_path": "models/marts/sales/model_0.sql",
            "package_name": "dbt_bouncer_test_project",
            "path": "marts/sales/model_0.sql",
            "unique_id": "model.dbt_bouncer_test_project.model_0",
        },
        [
            {
                "alias": "model_0",
                "config": {"materialized": "ephemeral"},
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.model_1"],
                },
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_0",
                "original_file_path": "models/marts/sales/model_0.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_0.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_0",
            },
            {
                "alias": "model_1",
                "config": {"materialized": "ephemeral"},
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.model_2"],
                },
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_1",
                "original_file_path": "models/marts/sales/model_1.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_1.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_1",
            },
            {
                "alias": "model_2",
                "config": {"materialized": "view"},
                "depends_on": {"nodes": []},
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_2",
                "original_file_path": "models/marts/sales/model_2.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_2.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_2",
            },
        ],
        does_not_raise(),
        id="within_limit",
    ),
    pytest.param(
        "manifest_obj",
        ["ephemeral", "view"],
        3,
        {
            "alias": "model_0",
            "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
            "fqn": ["dbt_bouncer_test_project", "model_1"],
            "name": "model_0",
            "original_file_path": "models/marts/sales/model_0.sql",
            "package_name": "dbt_bouncer_test_project",
            "path": "marts/sales/model_0.sql",
            "unique_id": "model.dbt_bouncer_test_project.model_0",
        },
        [
            {
                "alias": "model_0",
                "config": {"materialized": "ephemeral"},
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.model_1"],
                },
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_0",
                "original_file_path": "models/marts/sales/model_0.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_0.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_0",
            },
            {
                "alias": "model_1",
                "config": {"materialized": "ephemeral"},
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.model_2"],
                },
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_1",
                "original_file_path": "models/marts/sales/model_1.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_1.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_1",
            },
            {
                "alias": "model_2",
                "config": {"materialized": "view"},
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.model_3"],
                },
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_2",
                "original_file_path": "models/marts/sales/model_2.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_2.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_2",
            },
            {
                "alias": "model_3",
                "config": {"materialized": "view"},
                "depends_on": {"nodes": []},
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_3",
                "original_file_path": "models/marts/sales/model_3.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_3.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_3",
            },
        ],
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_limit",
    ),
]


@pytest.mark.parametrize(
    (
        "manifest_obj",
        "materializations_to_include",
        "max_chained_views",
        "model",
        "models",
        "expectation",
    ),
    _TEST_DATA_FOR_CHECK_MODEL_MAX_CHAINED_VIEWS,
    indirect=["manifest_obj", "model", "models"],
)
def test_check_model_max_chained_views(
    manifest_obj,
    materializations_to_include,
    max_chained_views,
    model,
    models,
    expectation,
):
    with expectation:
        CheckModelMaxChainedViews(
            manifest_obj=manifest_obj,
            materializations_to_include=materializations_to_include,
            max_chained_views=max_chained_views,
            model=model,
            models=models,
            name="check_model_max_chained_views",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_MAX_FANOUT = [
    pytest.param(
        1,
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        [
            {
                "alias": "stg_model_2",
                "depends_on": {
                    "nodes": [
                        "model.package_name.stg_model_1",
                    ],
                },
                "fqn": ["package_name", "stg_model_2"],
                "name": "stg_model_2",
                "original_file_path": "models/staging/stg_model_2.sql",
                "path": "staging/stg_model_2.sql",
                "unique_id": "model.package_name.stg_model_2",
            },
        ],
        does_not_raise(),
        id="within_fanout_limit",
    ),
    pytest.param(
        1,
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        [
            {
                "alias": "stg_model_2",
                "depends_on": {
                    "nodes": [
                        "model.package_name.stg_model_1",
                    ],
                },
                "fqn": ["package_name", "stg_model_2"],
                "name": "stg_model_2",
                "original_file_path": "models/staging/stg_model_2.sql",
                "path": "staging/stg_model_2.sql",
                "unique_id": "model.package_name.stg_model_2",
            },
            {
                "alias": "stg_model_3",
                "depends_on": {
                    "nodes": [
                        "model.package_name.stg_model_1",
                    ],
                },
                "fqn": ["package_name", "stg_model_3"],
                "name": "stg_model_3",
                "original_file_path": "models/staging/stg_model_3.sql",
                "path": "staging/stg_model_3.sql",
                "unique_id": "model.package_name.stg_model_3",
            },
        ],
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_fanout_limit",
    ),
]


@pytest.mark.parametrize(
    ("max_downstream_models", "model", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_MAX_FANOUT,
    indirect=["model", "models"],
)
def test_check_model_max_fanout(max_downstream_models, model, models, expectation):
    with expectation:
        CheckModelMaxFanout(
            max_downstream_models=max_downstream_models,
            model=model,
            models=models,
            name="check_model_max_fanout",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_MAX_UPSTREAM_DEPENDENCIES = [
    pytest.param(
        5,
        5,
        1,
        {
            "depends_on": {
                "macros": [
                    "macro.package_name.macro_1",
                    "macro.package_name.macro_2",
                    "macro.package_name.macro_3",
                    "macro.package_name.macro_4",
                    "macro.package_name.macro_5",
                ],
                "nodes": [
                    "model.package_name.stg_model_1",
                    "model.package_name.stg_model_2",
                    "model.package_name.stg_model_3",
                    "model.package_name.stg_model_4",
                    "model.package_name.stg_model_5",
                    "source.package_name.source_1",
                ],
            },
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        does_not_raise(),
        id="within_limits",
    ),
    pytest.param(
        5,
        5,
        1,
        {
            "depends_on": {
                "macros": [],
                "nodes": [],
            },
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        does_not_raise(),
        id="no_dependencies",
    ),
    pytest.param(
        5,
        5,
        1,
        {
            "depends_on": {
                "macros": [],
                "nodes": [
                    "source.package_name.source_1",
                    "source.package_name.source_2",
                ],
            },
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_source_limit",
    ),
    pytest.param(
        5,
        5,
        1,
        {
            "depends_on": {
                "macros": [
                    "macro.package_name.macro_1",
                    "macro.package_name.macro_2",
                    "macro.package_name.macro_3",
                    "macro.package_name.macro_4",
                    "macro.package_name.macro_5",
                    "macro.package_name.macro_6",
                ],
                "nodes": [],
            },
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_macro_limit",
    ),
    pytest.param(
        5,
        5,
        1,
        {
            "depends_on": {
                "macros": [],
                "nodes": [
                    "model.package_name.stg_model_1",
                    "model.package_name.stg_model_2",
                    "model.package_name.stg_model_3",
                    "model.package_name.stg_model_4",
                    "model.package_name.stg_model_5",
                    "model.package_name.stg_model_6",
                ],
            },
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_model_limit",
    ),
]


@pytest.mark.parametrize(
    (
        "max_upstream_macros",
        "max_upstream_models",
        "max_upstream_sources",
        "model",
        "expectation",
    ),
    _TEST_DATA_FOR_CHECK_MODEL_MAX_UPSTREAM_DEPENDENCIES,
    indirect=["model"],
)
def test_check_model_max_upstream_dependencies(
    max_upstream_macros,
    max_upstream_models,
    max_upstream_sources,
    model,
    expectation,
):
    with expectation:
        CheckModelMaxUpstreamDependencies(
            max_upstream_macros=max_upstream_macros,
            max_upstream_models=max_upstream_models,
            max_upstream_sources=max_upstream_sources,
            model=model,
            name="check_model_max_upstream_dependencies",
        ).execute()
