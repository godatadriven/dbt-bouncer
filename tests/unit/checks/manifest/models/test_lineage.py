import pytest

from dbt_bouncer.testing import check_fails, check_passes

_BASE_MODEL_MACROS = {
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
}


class TestCheckModelDependsOnMacros:
    @pytest.mark.parametrize(
        ("model", "required_macros", "criteria"),
        [
            pytest.param(
                {**_BASE_MODEL_MACROS},
                ["dbt.is_incremental"],
                "all",
                id="depends_on_required_macro",
            ),
            pytest.param(
                {
                    **_BASE_MODEL_MACROS,
                    "depends_on": {
                        "macros": [
                            "macro.dbt.is_incremental",
                            "macro.dbt.other_macro",
                        ],
                    },
                },
                ["dbt.is_incremental"],
                "any",
                id="depends_on_any_macro",
            ),
            pytest.param(
                {**_BASE_MODEL_MACROS},
                ["dbt.is_incremental", "dbt.other_macro"],
                "one",
                id="depends_on_one_macro",
            ),
        ],
    )
    def test_passes(self, model, required_macros, criteria):
        check_passes(
            "check_model_depends_on_macros",
            model=model,
            required_macros=required_macros,
            criteria=criteria,
        )

    @pytest.mark.parametrize(
        ("model", "required_macros", "criteria"),
        [
            pytest.param(
                {**_BASE_MODEL_MACROS},
                ["dbt.is_incremental", "dbt.other_macro"],
                "all",
                id="missing_one_required_macro",
            ),
            pytest.param(
                {**_BASE_MODEL_MACROS},
                ["dbt.other_macro"],
                "any",
                id="missing_any_required_macro",
            ),
            pytest.param(
                {
                    **_BASE_MODEL_MACROS,
                    "depends_on": {
                        "macros": [
                            "macro.dbt.is_incremental",
                            "macro.dbt.other_macro",
                        ],
                    },
                },
                ["dbt.is_incremental", "dbt.other_macro"],
                "one",
                id="depends_on_too_many_macros",
            ),
        ],
    )
    def test_fails(self, model, required_macros, criteria):
        check_fails(
            "check_model_depends_on_macros",
            model=model,
            required_macros=required_macros,
            criteria=criteria,
        )


class TestCheckModelDependsOnMacrosInvalidParam:
    def test_invalid_criteria_rejected(self):
        with pytest.raises(ValueError, match="'all', 'any' or 'one'"):
            check_passes(
                "check_model_depends_on_macros",
                model={},
                required_macros=["dbt.is_incremental"],
                criteria="alll",
            )


class TestCheckModelDependsOnMultipleSources:
    def test_passes(self):
        check_passes(
            "check_model_depends_on_multiple_sources",
            model={
                "alias": "model_2",
                "depends_on": {"nodes": ["source.package_name.source_1"]},
                "fqn": ["package_name", "model_2"],
                "name": "model_2",
                "original_file_path": "model_2.sql",
                "path": "model_2.sql",
                "unique_id": "model.package_name.model_2",
            },
        )

    def test_fails(self):
        check_fails(
            "check_model_depends_on_multiple_sources",
            model={
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
        )


class TestCheckModelDoesNotDirectlyJoinToSource:
    @pytest.mark.parametrize(
        "nodes",
        [
            pytest.param(
                ["source.package_name.source_1.table_1"],
                id="sources_only",
            ),
            pytest.param(
                [
                    "model.package_name.model_2",
                    "model.package_name.model_3",
                ],
                id="models_only",
            ),
            pytest.param([], id="no_upstream_dependencies"),
            pytest.param(
                [
                    "source.package_name.source_1.table_1",
                    "seed.package_name.seed_1",
                    "snapshot.package_name.snapshot_1",
                ],
                id="source_with_seed_and_snapshot_only",
            ),
        ],
    )
    def test_passes(self, nodes):
        check_passes(
            "check_model_does_not_directly_join_to_source",
            model={"depends_on": {"nodes": nodes}},
        )

    @pytest.mark.parametrize(
        "nodes",
        [
            pytest.param(
                [
                    "model.package_name.model_2",
                    "source.package_name.source_1.table_1",
                ],
                id="one_model_and_one_source",
            ),
            pytest.param(
                [
                    "model.package_name.model_2",
                    "seed.package_name.seed_1",
                    "source.package_name.source_1.table_1",
                    "source.package_name.source_2.table_1",
                ],
                id="multiple_models_and_sources",
            ),
        ],
    )
    def test_fails(self, nodes):
        check_fails(
            "check_model_does_not_directly_join_to_source",
            model={"depends_on": {"nodes": nodes}},
            match="references both a source",
        )


# model_3 (the model under test) <- model_2 <- model_1
_REJOIN_MODEL_1 = {"name": "model_1", "unique_id": "model.package_name.model_1"}
_REJOIN_MODEL_2 = {
    "depends_on": {"nodes": ["model.package_name.model_1"]},
    "name": "model_2",
    "unique_id": "model.package_name.model_2",
}
_REJOIN_MODEL_3 = {
    "depends_on": {
        "nodes": ["model.package_name.model_1", "model.package_name.model_2"],
    },
    "name": "model_3",
    "unique_id": "model.package_name.model_3",
}


class TestCheckModelDoesNotRejoinUpstreamConcepts:
    def test_passes_when_no_shared_ancestor(self):
        check_passes(
            "check_model_does_not_rejoin_upstream_concepts",
            model=_REJOIN_MODEL_3,
            ctx_models=[
                _REJOIN_MODEL_1,
                {**_REJOIN_MODEL_2, "depends_on": {"nodes": []}},
                _REJOIN_MODEL_3,
            ],
        )

    def test_passes_when_intermediate_has_other_consumers(self):
        # model_2 also feeds model_4, so it is a shared concept rather than a
        # single-consumer intermediate.
        check_passes(
            "check_model_does_not_rejoin_upstream_concepts",
            model=_REJOIN_MODEL_3,
            ctx_models=[
                _REJOIN_MODEL_1,
                _REJOIN_MODEL_2,
                _REJOIN_MODEL_3,
                {
                    "depends_on": {"nodes": ["model.package_name.model_2"]},
                    "name": "model_4",
                    "unique_id": "model.package_name.model_4",
                },
            ],
        )

    def test_passes_when_parent_is_a_source(self):
        check_passes(
            "check_model_does_not_rejoin_upstream_concepts",
            model={
                "depends_on": {
                    "nodes": [
                        "model.package_name.model_2",
                        "source.package_name.source_1.table_1",
                    ],
                },
                "name": "model_3",
                "unique_id": "model.package_name.model_3",
            },
            ctx_models=[_REJOIN_MODEL_2, _REJOIN_MODEL_3],
        )

    def test_fails(self):
        check_fails(
            "check_model_does_not_rejoin_upstream_concepts",
            model=_REJOIN_MODEL_3,
            ctx_models=[_REJOIN_MODEL_1, _REJOIN_MODEL_2, _REJOIN_MODEL_3],
            match="already depends on",
        )

    def test_fails_when_shared_ancestor_is_a_source(self):
        model_2 = {
            "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
            "name": "model_2",
            "unique_id": "model.package_name.model_2",
        }
        model_3 = {
            "depends_on": {
                "nodes": [
                    "model.package_name.model_2",
                    "source.package_name.source_1.table_1",
                ],
            },
            "name": "model_3",
            "unique_id": "model.package_name.model_3",
        }
        check_fails(
            "check_model_does_not_rejoin_upstream_concepts",
            model=model_3,
            ctx_models=[model_2, model_3],
            match="already depends on",
        )


class TestCheckModelHasExposure:
    def test_passes(self):
        check_passes(
            "check_model_has_exposure",
            model={
                "depends_on": {"nodes": ["source.package_name.source_1"]},
            },
            ctx_exposures=[
                {},  # default exposure depends on model_1
            ],
        )

    def test_fails(self):
        check_fails(
            "check_model_has_exposure",
            model={
                "depends_on": {"nodes": ["source.package_name.source_1"]},
            },
            ctx_exposures=[
                {
                    "depends_on": {"nodes": ["model.package_name.model_2"]},
                },
            ],
        )


class TestCheckModelHasNoUpstreamDependencies:
    @pytest.mark.parametrize(
        "model",
        [
            pytest.param(
                {
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                },
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
                id="depends_on_model",
            ),
        ],
    )
    def test_passes(self, model):
        check_passes("check_model_has_no_upstream_dependencies", model=model)

    def test_fails(self):
        check_fails(
            "check_model_has_no_upstream_dependencies",
            model={
                "depends_on": {"nodes": []},
            },
        )


class TestCheckModelMaterializationByFanout:
    def test_passes_few_downstreams(self):
        check_passes(
            "check_model_materialization_by_fanout",
            min_downstream_models=3,
            model={
                "alias": "hub",
                "config": {"materialized": "view"},
                "fqn": ["package_name", "hub"],
                "name": "hub",
                "original_file_path": "models/hub.sql",
                "path": "hub.sql",
                "unique_id": "model.package_name.hub",
            },
            ctx_models=[
                {
                    "alias": "child_1",
                    "depends_on": {"nodes": ["model.package_name.hub"]},
                    "fqn": ["package_name", "child_1"],
                    "name": "child_1",
                    "original_file_path": "models/child_1.sql",
                    "path": "child_1.sql",
                    "unique_id": "model.package_name.child_1",
                },
                {
                    "alias": "child_2",
                    "depends_on": {"nodes": ["model.package_name.hub"]},
                    "fqn": ["package_name", "child_2"],
                    "name": "child_2",
                    "original_file_path": "models/child_2.sql",
                    "path": "child_2.sql",
                    "unique_id": "model.package_name.child_2",
                },
            ],
        )

    def test_passes_durable_materialization(self):
        check_passes(
            "check_model_materialization_by_fanout",
            min_downstream_models=3,
            model={
                "alias": "hub",
                "config": {"materialized": "table"},
                "fqn": ["package_name", "hub"],
                "name": "hub",
                "original_file_path": "models/hub.sql",
                "path": "hub.sql",
                "unique_id": "model.package_name.hub",
            },
            ctx_models=[
                {
                    "alias": "child_1",
                    "depends_on": {"nodes": ["model.package_name.hub"]},
                    "fqn": ["package_name", "child_1"],
                    "name": "child_1",
                    "original_file_path": "models/child_1.sql",
                    "path": "child_1.sql",
                    "unique_id": "model.package_name.child_1",
                },
                {
                    "alias": "child_2",
                    "depends_on": {"nodes": ["model.package_name.hub"]},
                    "fqn": ["package_name", "child_2"],
                    "name": "child_2",
                    "original_file_path": "models/child_2.sql",
                    "path": "child_2.sql",
                    "unique_id": "model.package_name.child_2",
                },
                {
                    "alias": "child_3",
                    "depends_on": {"nodes": ["model.package_name.hub"]},
                    "fqn": ["package_name", "child_3"],
                    "name": "child_3",
                    "original_file_path": "models/child_3.sql",
                    "path": "child_3.sql",
                    "unique_id": "model.package_name.child_3",
                },
            ],
        )

    def test_fails_view_with_many_downstreams(self):
        check_fails(
            "check_model_materialization_by_fanout",
            min_downstream_models=3,
            model={
                "alias": "hub",
                "config": {"materialized": "view"},
                "fqn": ["package_name", "hub"],
                "name": "hub",
                "original_file_path": "models/hub.sql",
                "path": "hub.sql",
                "unique_id": "model.package_name.hub",
            },
            ctx_models=[
                {
                    "alias": "child_1",
                    "depends_on": {"nodes": ["model.package_name.hub"]},
                    "fqn": ["package_name", "child_1"],
                    "name": "child_1",
                    "original_file_path": "models/child_1.sql",
                    "path": "child_1.sql",
                    "unique_id": "model.package_name.child_1",
                },
                {
                    "alias": "child_2",
                    "depends_on": {"nodes": ["model.package_name.hub"]},
                    "fqn": ["package_name", "child_2"],
                    "name": "child_2",
                    "original_file_path": "models/child_2.sql",
                    "path": "child_2.sql",
                    "unique_id": "model.package_name.child_2",
                },
                {
                    "alias": "child_3",
                    "depends_on": {"nodes": ["model.package_name.hub"]},
                    "fqn": ["package_name", "child_3"],
                    "name": "child_3",
                    "original_file_path": "models/child_3.sql",
                    "path": "child_3.sql",
                    "unique_id": "model.package_name.child_3",
                },
            ],
        )


class TestCheckModelMaterializationByFanoutInvalidParam:
    @pytest.mark.parametrize(
        "min_downstream_models",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error(self, min_downstream_models):
        from dbt_bouncer.testing import _run_check

        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_materialization_by_fanout",
                min_downstream_models=min_downstream_models,
                model={},
                ctx_models=[{}],
            )


_CHAINED_VIEWS_MODELS_WITHIN_LIMIT = [
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
]

_CHAINED_VIEWS_MODELS_EXCEEDS_LIMIT = [
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
]

_CHAINED_VIEWS_MODEL_0 = {
    "alias": "model_0",
    "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
    "fqn": ["dbt_bouncer_test_project", "model_1"],
    "name": "model_0",
    "original_file_path": "models/marts/sales/model_0.sql",
    "package_name": "dbt_bouncer_test_project",
    "path": "marts/sales/model_0.sql",
    "unique_id": "model.dbt_bouncer_test_project.model_0",
}


class TestCheckModelMaxChainedViews:
    def test_passes(self):
        check_passes(
            "check_model_max_chained_views",
            materializations_to_include=["ephemeral", "view"],
            max_chained_views=3,
            model=_CHAINED_VIEWS_MODEL_0,
            ctx_models=_CHAINED_VIEWS_MODELS_WITHIN_LIMIT,
            ctx_manifest_obj={},
        )

    def test_fails(self):
        check_fails(
            "check_model_max_chained_views",
            materializations_to_include=["ephemeral", "view"],
            max_chained_views=3,
            model=_CHAINED_VIEWS_MODEL_0,
            ctx_models=_CHAINED_VIEWS_MODELS_EXCEEDS_LIMIT,
            ctx_manifest_obj={},
        )


class TestCheckModelMaxFanout:
    def test_passes(self):
        check_passes(
            "check_model_max_fanout",
            max_downstream_models=1,
            model={
                "alias": "stg_model_1",
                "fqn": ["package_name", "stg_model_1"],
                "name": "stg_model_1",
                "original_file_path": "models/staging/stg_model_1.sql",
                "path": "staging/stg_model_1.sql",
                "unique_id": "model.package_name.stg_model_1",
            },
            ctx_models=[
                {
                    "alias": "stg_model_2",
                    "depends_on": {
                        "nodes": ["model.package_name.stg_model_1"],
                    },
                    "fqn": ["package_name", "stg_model_2"],
                    "name": "stg_model_2",
                    "original_file_path": "models/staging/stg_model_2.sql",
                    "path": "staging/stg_model_2.sql",
                    "unique_id": "model.package_name.stg_model_2",
                },
            ],
        )

    def test_fails(self):
        check_fails(
            "check_model_max_fanout",
            max_downstream_models=1,
            model={
                "alias": "stg_model_1",
                "fqn": ["package_name", "stg_model_1"],
                "name": "stg_model_1",
                "original_file_path": "models/staging/stg_model_1.sql",
                "path": "staging/stg_model_1.sql",
                "unique_id": "model.package_name.stg_model_1",
            },
            ctx_models=[
                {
                    "alias": "stg_model_2",
                    "depends_on": {
                        "nodes": ["model.package_name.stg_model_1"],
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
                        "nodes": ["model.package_name.stg_model_1"],
                    },
                    "fqn": ["package_name", "stg_model_3"],
                    "name": "stg_model_3",
                    "original_file_path": "models/staging/stg_model_3.sql",
                    "path": "staging/stg_model_3.sql",
                    "unique_id": "model.package_name.stg_model_3",
                },
            ],
        )


class TestCheckModelMaxChainedViewsInvalidParam:
    @pytest.mark.parametrize(
        "max_chained_views",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error(self, max_chained_views):
        from dbt_bouncer.testing import _run_check

        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_max_chained_views",
                materializations_to_include=["ephemeral", "view"],
                max_chained_views=max_chained_views,
                model=_CHAINED_VIEWS_MODEL_0,
                ctx_models=_CHAINED_VIEWS_MODELS_WITHIN_LIMIT,
                ctx_manifest_obj={},
            )


class TestCheckModelMaxFanoutInvalidParam:
    @pytest.mark.parametrize(
        "max_downstream_models",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error(self, max_downstream_models):
        from dbt_bouncer.testing import _run_check

        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_max_fanout",
                max_downstream_models=max_downstream_models,
                model={},
                ctx_models=[{}],
            )


class TestCheckModelMaxUpstreamDependencies:
    @pytest.mark.parametrize(
        "model",
        [
            pytest.param(
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
                id="within_limits",
            ),
            pytest.param(
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
                id="no_dependencies",
            ),
        ],
    )
    def test_passes(self, model):
        check_passes(
            "check_model_max_upstream_dependencies",
            max_upstream_macros=5,
            max_upstream_models=5,
            max_upstream_sources=1,
            model=model,
        )

    @pytest.mark.parametrize(
        "model",
        [
            pytest.param(
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
                id="exceeds_source_limit",
            ),
            pytest.param(
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
                id="exceeds_macro_limit",
            ),
            pytest.param(
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
                id="exceeds_model_limit",
            ),
        ],
    )
    def test_fails(self, model):
        check_fails(
            "check_model_max_upstream_dependencies",
            max_upstream_macros=5,
            max_upstream_models=5,
            max_upstream_sources=1,
            model=model,
        )


class TestCheckModelMaxUpstreamDependenciesInvalidParam:
    @pytest.mark.parametrize(
        ("param_name", "kwargs"),
        [
            pytest.param(
                "max_upstream_macros",
                {
                    "max_upstream_macros": 0,
                    "max_upstream_models": 5,
                    "max_upstream_sources": 1,
                },
                id="max_upstream_macros_zero",
            ),
            pytest.param(
                "max_upstream_macros",
                {
                    "max_upstream_macros": -1,
                    "max_upstream_models": 5,
                    "max_upstream_sources": 1,
                },
                id="max_upstream_macros_negative",
            ),
            pytest.param(
                "max_upstream_models",
                {
                    "max_upstream_macros": 5,
                    "max_upstream_models": 0,
                    "max_upstream_sources": 1,
                },
                id="max_upstream_models_zero",
            ),
            pytest.param(
                "max_upstream_sources",
                {
                    "max_upstream_macros": 5,
                    "max_upstream_models": 5,
                    "max_upstream_sources": 0,
                },
                id="max_upstream_sources_zero",
            ),
        ],
    )
    def test_raises_value_error(self, param_name, kwargs):  # noqa: ARG002
        from dbt_bouncer.testing import _run_check

        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_max_upstream_dependencies",
                model={
                    "depends_on": {"macros": [], "nodes": []},
                },
                **kwargs,
            )


_DOWNSTREAM_CHILD_MODEL = {
    "depends_on": {"nodes": ["model.package_name.model_1"]},
    "name": "model_2",
    "unique_id": "model.package_name.model_2",
}


class TestCheckModelMinDownstreamModels:
    def test_passes_with_one_downstream_model(self):
        check_passes(
            "check_model_min_downstream_models",
            model={},  # default model is model_1
            ctx_models=[_DOWNSTREAM_CHILD_MODEL],
        )

    def test_passes_with_downstream_snapshot_only(self):
        check_passes(
            "check_model_min_downstream_models",
            model={},
            ctx_snapshots=[
                {"depends_on": {"nodes": ["model.package_name.model_1"]}},
            ],
        )

    def test_passes_with_higher_minimum(self):
        check_passes(
            "check_model_min_downstream_models",
            model={},
            min_number_of_models=2,
            ctx_models=[
                _DOWNSTREAM_CHILD_MODEL,
                {
                    "depends_on": {"nodes": ["model.package_name.model_1"]},
                    "name": "model_3",
                    "unique_id": "model.package_name.model_3",
                },
            ],
        )

    def test_fails_when_dead(self):
        check_fails(
            "check_model_min_downstream_models",
            model={},
            ctx_models=[
                {
                    "depends_on": {"nodes": ["model.package_name.model_99"]},
                    "name": "model_2",
                    "unique_id": "model.package_name.model_2",
                },
            ],
            match="fewer than the minimum",
        )

    def test_fails_when_below_higher_minimum(self):
        check_fails(
            "check_model_min_downstream_models",
            model={},
            min_number_of_models=2,
            ctx_models=[_DOWNSTREAM_CHILD_MODEL],
            match="fewer than the minimum",
        )


class TestCheckModelMinDownstreamModelsInvalidParam:
    def test_zero_rejected(self):
        with pytest.raises(ValueError, match="greater than 0"):
            check_passes(
                "check_model_min_downstream_models",
                model={},
                min_number_of_models=0,
            )
