import pytest

from dbt_bouncer.testing import _run_check, check_fails, check_passes


def _model(
    name: str,
    *,
    package: str = "package_name",
    nodes: list[str] | None = None,
    macros: list[str] | None = None,
    materialized: str | None = None,
) -> dict:
    """Build a model override dict.

    Args:
        name: The model name, also used to derive `unique_id`, `fqn` and paths.
        package: The dbt package the model belongs to.
        nodes: `depends_on.nodes` entries, or None to omit the key.
        macros: `depends_on.macros` entries, or None to omit the key.
        materialized: `config.materialized`, or None to omit `config`.

    Returns:
        dict: A manifest model node dict.

    """
    model: dict = {
        "alias": name,
        "fqn": [package, name],
        "name": name,
        "original_file_path": f"models/{name}.sql",
        "package_name": package,
        "path": f"{name}.sql",
        "unique_id": f"model.{package}.{name}",
    }
    depends_on: dict = {}
    if nodes is not None:
        depends_on["nodes"] = nodes
    if macros is not None:
        depends_on["macros"] = macros
    if depends_on:
        model["depends_on"] = depends_on
    if materialized is not None:
        model["config"] = {"materialized": materialized}
    return model


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

_TWO_MACROS = {
    **_BASE_MODEL_MACROS,
    "depends_on": {
        "macros": ["macro.dbt.is_incremental", "macro.dbt.other_macro"],
    },
}


class TestCheckModelDependsOnMacros:
    @pytest.mark.parametrize(
        ("model", "required_macros", "criteria", "check_fn"),
        [
            pytest.param(
                _BASE_MODEL_MACROS,
                ["dbt.is_incremental"],
                "all",
                check_passes,
                id="depends_on_required_macro",
            ),
            pytest.param(
                _TWO_MACROS,
                ["dbt.is_incremental"],
                "any",
                check_passes,
                id="depends_on_any_macro",
            ),
            pytest.param(
                _BASE_MODEL_MACROS,
                ["dbt.is_incremental", "dbt.other_macro"],
                "one",
                check_passes,
                id="depends_on_one_macro",
            ),
            pytest.param(
                _TWO_MACROS,
                ["dbt.is_incremental", "dbt.other_macro"],
                "all",
                check_passes,
                id="depends_on_all_of_two_macros",
            ),
            pytest.param(
                _BASE_MODEL_MACROS,
                ["dbt.is_incremental", "dbt.other_macro"],
                "all",
                check_fails,
                id="missing_one_required_macro",
            ),
            pytest.param(
                _BASE_MODEL_MACROS,
                ["dbt.other_macro"],
                "any",
                check_fails,
                id="missing_any_required_macro",
            ),
            pytest.param(
                _TWO_MACROS,
                ["dbt.is_incremental", "dbt.other_macro"],
                "one",
                check_fails,
                id="depends_on_too_many_macros",
            ),
            pytest.param(
                _BASE_MODEL_MACROS,
                ["dbt.other_macro"],
                "one",
                check_fails,
                id="depends_on_none_of_the_macros_with_criteria_one",
            ),
        ],
    )
    def test_check_model_depends_on_macros(
        self, model, required_macros, criteria, check_fn
    ):
        check_fn(
            "check_model_depends_on_macros",
            model=model,
            required_macros=required_macros,
            criteria=criteria,
        )

    def test_criteria_defaults_to_all(self):
        check_fails(
            "check_model_depends_on_macros",
            model=_BASE_MODEL_MACROS,
            required_macros=["dbt.is_incremental", "dbt.other_macro"],
        )

    def test_invalid_criteria_rejected(self):
        with pytest.raises(ValueError, match="'all', 'any' or 'one'"):
            _run_check(
                "check_model_depends_on_macros",
                model={},
                required_macros=["dbt.is_incremental"],
                criteria="alll",
            )


class TestCheckModelDependsOnMultipleSources:
    @pytest.mark.parametrize(
        ("nodes", "check_fn"),
        [
            pytest.param(
                ["source.package_name.source_1"], check_passes, id="one_source"
            ),
            pytest.param([], check_passes, id="no_upstream_dependencies"),
            pytest.param(
                ["model.package_name.model_1", "source.package_name.source_1"],
                check_passes,
                id="one_source_and_one_model",
            ),
            pytest.param(
                ["source.package_name.source_1", "source.package_name.source_2"],
                check_fails,
                id="two_sources",
            ),
            pytest.param(
                [
                    "source.package_name.source_1",
                    "source.package_name.source_2",
                    "source.package_name.source_3",
                ],
                check_fails,
                id="three_sources",
            ),
        ],
    )
    def test_check_model_depends_on_multiple_sources(self, nodes, check_fn):
        check_fn(
            "check_model_depends_on_multiple_sources",
            model=_model("model_2", nodes=nodes),
        )


class TestCheckModelDoesNotDirectlyJoinToSource:
    @pytest.mark.parametrize(
        ("nodes", "check_fn"),
        [
            pytest.param(
                ["source.package_name.source_1.table_1"],
                check_passes,
                id="sources_only",
            ),
            pytest.param(
                ["model.package_name.model_2", "model.package_name.model_3"],
                check_passes,
                id="models_only",
            ),
            pytest.param([], check_passes, id="no_upstream_dependencies"),
            pytest.param(
                [
                    "source.package_name.source_1.table_1",
                    "seed.package_name.seed_1",
                    "snapshot.package_name.snapshot_1",
                ],
                check_passes,
                id="source_with_seed_and_snapshot_only",
            ),
            pytest.param(
                [
                    "model.package_name.model_2",
                    "source.package_name.source_1.table_1",
                ],
                check_fails,
                id="one_model_and_one_source",
            ),
            pytest.param(
                [
                    "model.package_name.model_2",
                    "seed.package_name.seed_1",
                    "source.package_name.source_1.table_1",
                    "source.package_name.source_2.table_1",
                ],
                check_fails,
                id="multiple_models_and_sources",
            ),
        ],
    )
    def test_check_model_does_not_directly_join_to_source(self, nodes, check_fn):
        check_fn(
            "check_model_does_not_directly_join_to_source",
            model={"depends_on": {"nodes": nodes}},
        )

    def test_failure_message(self):
        check_fails(
            "check_model_does_not_directly_join_to_source",
            model={
                "depends_on": {
                    "nodes": [
                        "model.package_name.model_2",
                        "source.package_name.source_1.table_1",
                    ]
                }
            },
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

_REJOIN_MODEL_2_FROM_SOURCE = {
    "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
    "name": "model_2",
    "unique_id": "model.package_name.model_2",
}
_REJOIN_MODEL_3_FROM_SOURCE = {
    "depends_on": {
        "nodes": [
            "model.package_name.model_2",
            "source.package_name.source_1.table_1",
        ],
    },
    "name": "model_3",
    "unique_id": "model.package_name.model_3",
}


class TestCheckModelDoesNotRejoinUpstreamConcepts:
    @pytest.mark.parametrize(
        ("model", "models_list", "check_fn"),
        [
            pytest.param(
                _REJOIN_MODEL_3,
                [
                    _REJOIN_MODEL_1,
                    {**_REJOIN_MODEL_2, "depends_on": {"nodes": []}},
                    _REJOIN_MODEL_3,
                ],
                check_passes,
                id="no_shared_ancestor",
            ),
            # model_2 also feeds model_4, so it is a shared concept rather than a
            # single-consumer intermediate.
            pytest.param(
                _REJOIN_MODEL_3,
                [
                    _REJOIN_MODEL_1,
                    _REJOIN_MODEL_2,
                    _REJOIN_MODEL_3,
                    {
                        "depends_on": {"nodes": ["model.package_name.model_2"]},
                        "name": "model_4",
                        "unique_id": "model.package_name.model_4",
                    },
                ],
                check_passes,
                id="intermediate_has_other_consumers",
            ),
            pytest.param(
                _REJOIN_MODEL_3_FROM_SOURCE,
                [_REJOIN_MODEL_2, _REJOIN_MODEL_3],
                check_passes,
                id="parent_is_a_source",
            ),
            pytest.param(
                _REJOIN_MODEL_3,
                [_REJOIN_MODEL_1, _REJOIN_MODEL_2, _REJOIN_MODEL_3],
                check_fails,
                id="rejoins_upstream_model",
            ),
            pytest.param(
                _REJOIN_MODEL_3_FROM_SOURCE,
                [_REJOIN_MODEL_2_FROM_SOURCE, _REJOIN_MODEL_3_FROM_SOURCE],
                check_fails,
                id="shared_ancestor_is_a_source",
            ),
        ],
    )
    def test_check_model_does_not_rejoin_upstream_concepts(
        self, model, models_list, check_fn
    ):
        check_fn(
            "check_model_does_not_rejoin_upstream_concepts",
            model=model,
            ctx_models=models_list,
        )

    def test_failure_message(self):
        check_fails(
            "check_model_does_not_rejoin_upstream_concepts",
            model=_REJOIN_MODEL_3,
            ctx_models=[_REJOIN_MODEL_1, _REJOIN_MODEL_2, _REJOIN_MODEL_3],
            match="already depends on",
        )


class TestCheckModelHasExposure:
    @pytest.mark.parametrize(
        ("exposures", "check_fn"),
        [
            # The default exposure depends on model_1, the model under test.
            pytest.param([{}], check_passes, id="model_has_an_exposure"),
            pytest.param(
                [{"depends_on": {"nodes": ["model.package_name.model_2"]}}],
                check_fails,
                id="exposure_for_a_different_model",
            ),
            pytest.param([], check_fails, id="no_exposures"),
        ],
    )
    def test_check_model_has_exposure(self, exposures, check_fn):
        check_fn(
            "check_model_has_exposure",
            model={"depends_on": {"nodes": ["source.package_name.source_1"]}},
            ctx_exposures=exposures,
        )


class TestCheckModelHasNoUpstreamDependencies:
    @pytest.mark.parametrize(
        ("model", "check_fn"),
        [
            pytest.param(
                {"depends_on": {"nodes": ["source.package_name.source_1"]}},
                check_passes,
                id="depends_on_source",
            ),
            pytest.param(
                _model("int_model_1", nodes=["model.package_name.stg_model_1"]),
                check_passes,
                id="depends_on_model",
            ),
            pytest.param(
                {"depends_on": {"nodes": []}}, check_fails, id="no_upstream_nodes"
            ),
        ],
    )
    def test_check_model_has_no_upstream_dependencies(self, model, check_fn):
        check_fn("check_model_has_no_upstream_dependencies", model=model)


def _children_of(hub: str, count: int) -> list[dict]:
    """Build `count` models that each depend on `hub`.

    Returns:
        list[dict]: The downstream model node dicts.

    """
    return [
        _model(f"child_{i}", nodes=[f"model.package_name.{hub}"])
        for i in range(1, count + 1)
    ]


class TestCheckModelMaterializationByFanout:
    @pytest.mark.parametrize(
        ("materialized", "num_children", "check_fn"),
        [
            pytest.param("view", 2, check_passes, id="few_downstreams"),
            pytest.param("table", 3, check_passes, id="durable_materialization"),
            pytest.param("incremental", 3, check_passes, id="incremental_is_durable"),
            pytest.param("view", 3, check_fails, id="view_with_many_downstreams"),
            pytest.param(
                "ephemeral", 3, check_fails, id="ephemeral_with_many_downstreams"
            ),
        ],
    )
    def test_check_model_materialization_by_fanout(
        self, materialized, num_children, check_fn
    ):
        check_fn(
            "check_model_materialization_by_fanout",
            min_downstream_models=3,
            model=_model("hub", materialized=materialized),
            ctx_models=_children_of("hub", num_children),
        )

    @pytest.mark.parametrize(
        "min_downstream_models",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error(self, min_downstream_models):
        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_materialization_by_fanout",
                min_downstream_models=min_downstream_models,
                model={},
                ctx_models=[{}],
            )


_TEST_PROJECT = "dbt_bouncer_test_project"


def _chain_model(name: str, materialized: str, upstream: str | None = None) -> dict:
    """Build a model in the default test project, optionally depending on `upstream`.

    Returns:
        dict: A manifest model node dict.

    """
    return _model(
        name,
        package=_TEST_PROJECT,
        materialized=materialized,
        nodes=[f"model.{_TEST_PROJECT}.{upstream}"] if upstream else [],
    )


# model_0 <- model_1 <- model_2, all non-table: a chain of 3.
_CHAINED_VIEWS_MODELS_WITHIN_LIMIT = [
    _chain_model("model_0", "ephemeral", "model_1"),
    _chain_model("model_1", "ephemeral", "model_2"),
    _chain_model("model_2", "view"),
]

# model_0 <- model_1 <- model_2 <- model_3: a chain of 4.
_CHAINED_VIEWS_MODELS_EXCEEDS_LIMIT = [
    _chain_model("model_0", "ephemeral", "model_1"),
    _chain_model("model_1", "ephemeral", "model_2"),
    _chain_model("model_2", "view", "model_3"),
    _chain_model("model_3", "view"),
]

_CHAINED_VIEWS_MODEL_0 = _model(
    "model_0", package=_TEST_PROJECT, nodes=[f"model.{_TEST_PROJECT}.model_1"]
)


class TestCheckModelMaxChainedViews:
    @pytest.mark.parametrize(
        ("models_list", "check_fn"),
        [
            pytest.param(
                _CHAINED_VIEWS_MODELS_WITHIN_LIMIT, check_passes, id="within_limit"
            ),
            pytest.param(
                _CHAINED_VIEWS_MODELS_EXCEEDS_LIMIT, check_fails, id="exceeds_limit"
            ),
        ],
    )
    def test_check_model_max_chained_views(self, models_list, check_fn):
        check_fn(
            "check_model_max_chained_views",
            materializations_to_include=["ephemeral", "view"],
            max_chained_views=3,
            model=_CHAINED_VIEWS_MODEL_0,
            ctx_models=models_list,
            ctx_manifest_obj={},
        )

    def test_passes_when_model_is_absent_from_the_model_list(self):
        # The recursion looks each unique_id up in the model index and skips any
        # that is missing, so a model absent from ctx.models has no upstream
        # chain to walk.
        check_passes(
            "check_model_max_chained_views",
            materializations_to_include=["ephemeral", "view"],
            max_chained_views=1,
            model=_model("orphan", package=_TEST_PROJECT),
            ctx_models=[],
            ctx_manifest_obj={},
        )

    def test_passes_when_an_upstream_model_is_absent_from_the_model_list(self):
        model_0 = _chain_model("model_0", "view", "gone")
        check_passes(
            "check_model_max_chained_views",
            materializations_to_include=["view"],
            max_chained_views=1,
            model=model_0,
            ctx_models=[model_0],
            ctx_manifest_obj={},
        )

    def test_passes_when_every_upstream_model_is_a_table(self):
        # Upstream models whose materialization is not in
        # `materializations_to_include` are not counted towards the chain.
        top = _model(
            "top",
            package=_TEST_PROJECT,
            materialized="view",
            nodes=[
                f"model.{_TEST_PROJECT}.upstream_1",
                f"model.{_TEST_PROJECT}.upstream_2",
            ],
        )
        check_passes(
            "check_model_max_chained_views",
            materializations_to_include=["view"],
            max_chained_views=1,
            model=top,
            ctx_models=[
                top,
                _chain_model("upstream_1", "table"),
                _chain_model("upstream_2", "table"),
            ],
            ctx_manifest_obj={},
        )

    def test_package_name_overrides_the_manifest_project_name(self):
        # Upstream unique_ids are filtered by package, so a chain in another
        # package is only walked when `package_name` names it.
        models_list = [
            _model(
                "top",
                package="other_pkg",
                materialized="view",
                nodes=["model.other_pkg.mid"],
            ),
            _model(
                "mid",
                package="other_pkg",
                materialized="view",
                nodes=["model.other_pkg.leaf"],
            ),
            _model("leaf", package="other_pkg", materialized="view"),
        ]
        check_fails(
            "check_model_max_chained_views",
            materializations_to_include=["view"],
            max_chained_views=1,
            package_name="other_pkg",
            model=models_list[0],
            ctx_models=models_list,
            ctx_manifest_obj={},
        )
        # Without the override the manifest project name does not match, so
        # nothing upstream is considered.
        check_passes(
            "check_model_max_chained_views",
            materializations_to_include=["view"],
            max_chained_views=1,
            model=models_list[0],
            ctx_models=models_list,
            ctx_manifest_obj={},
        )

    def test_failure_message(self):
        check_fails(
            "check_model_max_chained_views",
            materializations_to_include=["ephemeral", "view"],
            max_chained_views=3,
            model=_CHAINED_VIEWS_MODEL_0,
            ctx_models=_CHAINED_VIEWS_MODELS_EXCEEDS_LIMIT,
            ctx_manifest_obj={},
            match=r"has more than 3 upstream dependents that are not tables\.",
        )

    @pytest.mark.parametrize(
        "max_chained_views",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error(self, max_chained_views):
        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_max_chained_views",
                materializations_to_include=["ephemeral", "view"],
                max_chained_views=max_chained_views,
                model=_CHAINED_VIEWS_MODEL_0,
                ctx_models=_CHAINED_VIEWS_MODELS_WITHIN_LIMIT,
                ctx_manifest_obj={},
            )


class TestCheckModelMaxFanout:
    @pytest.mark.parametrize(
        ("max_downstream_models", "num_children", "check_fn"),
        [
            pytest.param(1, 1, check_passes, id="at_limit"),
            pytest.param(2, 1, check_passes, id="below_limit"),
            pytest.param(1, 0, check_passes, id="no_downstream_models"),
            pytest.param(1, 2, check_fails, id="one_above_limit"),
            pytest.param(2, 3, check_fails, id="exceeds_higher_limit"),
        ],
    )
    def test_check_model_max_fanout(
        self, max_downstream_models, num_children, check_fn
    ):
        check_fn(
            "check_model_max_fanout",
            max_downstream_models=max_downstream_models,
            model=_model("stg_model_1"),
            ctx_models=_children_of("stg_model_1", num_children),
        )

    @pytest.mark.parametrize(
        "max_downstream_models",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error(self, max_downstream_models):
        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_max_fanout",
                max_downstream_models=max_downstream_models,
                model={},
                ctx_models=[{}],
            )


def _upstream(*, macros: int = 0, models: int = 0, sources: int = 0) -> dict:
    """Build a model with the given number of upstream macros, models and sources.

    Returns:
        dict: A manifest model node dict.

    """
    return _model(
        "stg_model_1",
        macros=[f"macro.package_name.macro_{i}" for i in range(1, macros + 1)],
        nodes=[f"model.package_name.stg_model_{i}" for i in range(1, models + 1)]
        + [f"source.package_name.source_{i}" for i in range(1, sources + 1)],
    )


class TestCheckModelMaxUpstreamDependencies:
    @pytest.mark.parametrize(
        ("model", "check_fn"),
        [
            pytest.param(
                _upstream(macros=5, models=5, sources=1),
                check_passes,
                id="at_every_limit",
            ),
            pytest.param(_upstream(), check_passes, id="no_dependencies"),
            pytest.param(
                _upstream(macros=4, models=4), check_passes, id="below_limits"
            ),
            # `depends_on` absent or None short-circuits to zero counts rather
            # than raising.
            pytest.param({"depends_on": None}, check_passes, id="depends_on_none"),
            pytest.param({}, check_passes, id="depends_on_absent"),
            pytest.param(_upstream(sources=2), check_fails, id="exceeds_source_limit"),
            pytest.param(_upstream(macros=6), check_fails, id="exceeds_macro_limit"),
            pytest.param(_upstream(models=6), check_fails, id="exceeds_model_limit"),
        ],
    )
    def test_check_model_max_upstream_dependencies(self, model, check_fn):
        check_fn(
            "check_model_max_upstream_dependencies",
            max_upstream_macros=5,
            max_upstream_models=5,
            max_upstream_sources=1,
            model=model,
        )

    @pytest.mark.parametrize(
        ("model", "match"),
        [
            pytest.param(
                _upstream(macros=6),
                r"has 6 upstream macros, which is more than the permitted maximum of 5\.",
                id="macros",
            ),
            pytest.param(
                _upstream(models=6),
                r"has 6 upstream models, which is more than the permitted maximum of 5\.",
                id="models",
            ),
            pytest.param(
                _upstream(sources=2),
                r"has 2 upstream sources, which is more than the permitted maximum of 1\.",
                id="sources",
            ),
        ],
    )
    def test_failure_messages(self, model, match):
        check_fails(
            "check_model_max_upstream_dependencies",
            max_upstream_macros=5,
            max_upstream_models=5,
            max_upstream_sources=1,
            model=model,
            match=match,
        )

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param(
                {
                    "max_upstream_macros": 0,
                    "max_upstream_models": 5,
                    "max_upstream_sources": 1,
                },
                id="max_upstream_macros_zero",
            ),
            pytest.param(
                {
                    "max_upstream_macros": -1,
                    "max_upstream_models": 5,
                    "max_upstream_sources": 1,
                },
                id="max_upstream_macros_negative",
            ),
            pytest.param(
                {
                    "max_upstream_macros": 5,
                    "max_upstream_models": 0,
                    "max_upstream_sources": 1,
                },
                id="max_upstream_models_zero",
            ),
            pytest.param(
                {
                    "max_upstream_macros": 5,
                    "max_upstream_models": 5,
                    "max_upstream_sources": 0,
                },
                id="max_upstream_sources_zero",
            ),
        ],
    )
    def test_raises_value_error(self, kwargs):
        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_max_upstream_dependencies",
                model={"depends_on": {"macros": [], "nodes": []}},
                **kwargs,
            )


_DOWNSTREAM_CHILD_MODEL = {
    "depends_on": {"nodes": ["model.package_name.model_1"]},
    "name": "model_2",
    "unique_id": "model.package_name.model_2",
}
_DOWNSTREAM_CHILD_MODEL_3 = {
    "depends_on": {"nodes": ["model.package_name.model_1"]},
    "name": "model_3",
    "unique_id": "model.package_name.model_3",
}
_DOWNSTREAM_SNAPSHOT = {"depends_on": {"nodes": ["model.package_name.model_1"]}}


class TestCheckModelMinDownstreamModels:
    @pytest.mark.parametrize(
        ("min_number_of_models", "models_list", "snapshots", "check_fn"),
        [
            pytest.param(
                1,
                [_DOWNSTREAM_CHILD_MODEL],
                [],
                check_passes,
                id="one_downstream_model",
            ),
            pytest.param(
                1,
                [],
                [_DOWNSTREAM_SNAPSHOT],
                check_passes,
                id="downstream_snapshot_only",
            ),
            pytest.param(
                2,
                [_DOWNSTREAM_CHILD_MODEL, _DOWNSTREAM_CHILD_MODEL_3],
                [],
                check_passes,
                id="two_downstream_models_at_higher_minimum",
            ),
            # Exercises both terms of the count together: one downstream model
            # plus one downstream snapshot must sum to 2.
            pytest.param(
                2,
                [_DOWNSTREAM_CHILD_MODEL],
                [_DOWNSTREAM_SNAPSHOT],
                check_passes,
                id="mixed_model_and_snapshot_consumers",
            ),
            pytest.param(
                1,
                [
                    {
                        "depends_on": {"nodes": ["model.package_name.model_99"]},
                        "name": "model_2",
                        "unique_id": "model.package_name.model_2",
                    }
                ],
                [],
                check_fails,
                id="no_downstream_consumers",
            ),
            pytest.param(1, [], [], check_fails, id="no_models_or_snapshots_at_all"),
            pytest.param(
                2,
                [_DOWNSTREAM_CHILD_MODEL],
                [],
                check_fails,
                id="below_higher_minimum",
            ),
        ],
    )
    def test_check_model_min_downstream_models(
        self, min_number_of_models, models_list, snapshots, check_fn
    ):
        check_fn(
            "check_model_min_downstream_models",
            model={},  # default model is model_1
            min_number_of_models=min_number_of_models,
            ctx_models=models_list,
            ctx_snapshots=snapshots,
        )

    def test_failure_message(self):
        check_fails(
            "check_model_min_downstream_models",
            model={},
            ctx_models=[],
            match="fewer than the minimum",
        )

    def test_zero_rejected(self):
        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_min_downstream_models",
                model={},
                min_number_of_models=0,
            )
