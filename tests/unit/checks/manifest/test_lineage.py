import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckLineagePermittedUpstreamModels:
    @pytest.mark.parametrize(
        ("model_overrides", "ctx_models", "upstream_path_pattern", "check_fn"),
        [
            pytest.param(
                {
                    "alias": "int_model",
                    "depends_on": {
                        "nodes": ["model.dbt_bouncer_test_project.stg_model_1"],
                    },
                    "fqn": ["dbt_bouncer_test_project", "int_model"],
                    "name": "int_model",
                    "original_file_path": "intermediate/int_model.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "intermediate/int_model.sql",
                    "unique_id": "model.dbt_bouncer_test_project.int_model",
                },
                [
                    {
                        "alias": "stg_model_1",
                        "fqn": ["dbt_bouncer_test_project", "stg_model_1"],
                        "name": "stg_model_1",
                        "original_file_path": "staging/stg_model_1.sql",
                        "package_name": "dbt_bouncer_test_project",
                        "path": "staging/stg_model_1.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_model_1",
                    },
                ],
                "^staging",
                check_passes,
                id="upstream_matches_pattern",
            ),
            pytest.param(
                {
                    "alias": "int_model",
                    "depends_on": {"nodes": ["model.some_other_package.stg_model"]},
                    "fqn": ["dbt_bouncer_test_project", "int_model"],
                    "name": "int_model",
                    "original_file_path": "intermediate/int_model.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "intermediate/int_model.sql",
                    "unique_id": "model.dbt_bouncer_test_project.int_model",
                },
                [],
                "^staging",
                check_passes,
                id="cross_package_dependency_skipped",
            ),
            pytest.param(
                {
                    "alias": "mart_model",
                    "depends_on": {
                        "nodes": ["model.dbt_bouncer_test_project.mart_other_model"],
                    },
                    "fqn": ["dbt_bouncer_test_project", "mart_model"],
                    "name": "mart_model",
                    "original_file_path": "marts/mart_model.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "marts/mart_model.sql",
                    "unique_id": "model.dbt_bouncer_test_project.mart_model",
                },
                [
                    {
                        "alias": "mart_other_model",
                        "fqn": ["dbt_bouncer_test_project", "mart_other_model"],
                        "name": "mart_other_model",
                        "original_file_path": "marts/mart_other_model.sql",
                        "package_name": "dbt_bouncer_test_project",
                        "path": "marts/mart_other_model.sql",
                        "unique_id": "model.dbt_bouncer_test_project.mart_other_model",
                    },
                ],
                "^intermediate",
                check_fails,
                id="upstream_violates_pattern",
            ),
        ],
    )
    def test_check_lineage_permitted_upstream_models(
        self,
        model_overrides,
        ctx_models,
        upstream_path_pattern,
        check_fn,
    ):
        check_fn(
            "check_lineage_permitted_upstream_models",
            model=model_overrides,
            upstream_path_pattern=upstream_path_pattern,
            ctx_models=ctx_models,
        )


_MART_MODEL = {
    "depends_on": {
        "nodes": [
            "model.package_name.int_model_1",
            "source.package_name.domain__orders.orders",
        ]
    },
    "original_file_path": "models/marts/mart_model.sql",
    "path": "marts/mart_model.sql",
}


def _upstream_source(database: str | None, schema: str) -> dict:
    return {
        "database": database,
        "fqn": ["package_name", "domain__orders", "orders"],
        "identifier": "orders",
        "name": "orders",
        "schema": schema,
        "source_name": "domain__orders",
        "unique_id": "source.package_name.domain__orders.orders",
    }


class TestCheckLineagePermittedUpstreamSources:
    @pytest.mark.parametrize(
        ("model_overrides", "ctx_sources", "patterns", "check_fn"),
        [
            pytest.param(
                _MART_MODEL,
                [_upstream_source("dmn_sales", "domain__marts")],
                {"database_pattern": "^dmn", "schema_pattern": "^domain__"},
                check_passes,
                id="database_and_schema_match",
            ),
            pytest.param(
                _MART_MODEL,
                [_upstream_source("raw", "domain__marts")],
                {"schema_pattern": "^domain__"},
                check_passes,
                id="schema_only_match",
            ),
            pytest.param(
                {**_MART_MODEL, "depends_on": {"nodes": []}},
                [_upstream_source("raw", "crm")],
                {"database_pattern": "^dmn"},
                check_passes,
                id="no_source_dependency",
            ),
            pytest.param(
                _MART_MODEL,
                [_upstream_source("raw", "domain__marts")],
                {"database_pattern": "^dmn", "schema_pattern": "^domain__"},
                check_fails,
                id="database_mismatch",
            ),
            pytest.param(
                _MART_MODEL,
                [_upstream_source("dmn_sales", "staging")],
                {"database_pattern": "^dmn", "schema_pattern": "^domain__"},
                check_fails,
                id="schema_mismatch",
            ),
            pytest.param(
                _MART_MODEL,
                [_upstream_source(None, "domain__marts")],
                {"database_pattern": "^dmn"},
                check_fails,
                id="missing_database",
            ),
        ],
    )
    def test_check_lineage_permitted_upstream_sources(
        self, model_overrides, ctx_sources, patterns, check_fn
    ):
        check_fn(
            "check_lineage_permitted_upstream_sources",
            model=model_overrides,
            ctx_sources=ctx_sources,
            **patterns,
        )

    def test_requires_database_or_schema_pattern(self):
        with pytest.raises(
            ValueError, match=r"database_pattern.*schema_pattern.*must be supplied"
        ):
            check_passes(
                "check_lineage_permitted_upstream_sources",
                model=_MART_MODEL,
                ctx_sources=[_upstream_source("dmn_sales", "domain__marts")],
            )


class TestCheckLineageSeedCannotBeUsed:
    @pytest.mark.parametrize(
        ("model_overrides", "check_fn"),
        [
            pytest.param(
                {
                    "alias": "int_model_2",
                    "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
                    "fqn": ["package_name", "int_model_2"],
                    "name": "int_model_2",
                    "original_file_path": "intermediate/int_model_2.sql",
                    "path": "intermediate/int_model_2.sql",
                    "unique_id": "model.package_name.int_model_2",
                },
                check_passes,
                id="no_seed_dependency",
            ),
            pytest.param(
                {
                    "alias": "int_model_2",
                    "depends_on": {"nodes": ["seed.package_name.seed_1"]},
                    "fqn": ["package_name", "int_model_2"],
                    "name": "int_model_2",
                    "original_file_path": "intermediate/int_model_2.sql",
                    "path": "intermediate/int_model_2.sql",
                    "unique_id": "model.package_name.int_model_2",
                },
                check_fails,
                id="has_seed_dependency",
            ),
        ],
    )
    def test_check_lineage_seed_cannot_be_used(self, model_overrides, check_fn):
        check_fn(
            "check_lineage_seed_cannot_be_used",
            model=model_overrides,
        )


class TestCheckLineageSourceCannotBeUsed:
    @pytest.mark.parametrize(
        ("model_overrides", "check_fn"),
        [
            pytest.param(
                {
                    "alias": "int_model_2",
                    "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
                    "fqn": ["package_name", "int_model_2"],
                    "name": "int_model_2",
                    "original_file_path": "intermediate/int_model_2.sql",
                    "path": "intermediate/int_model_2.sql",
                    "unique_id": "model.package_name.int_model_2",
                },
                check_passes,
                id="no_source_dependency",
            ),
            pytest.param(
                {
                    "alias": "int_model_2",
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                    "fqn": ["package_name", "int_model_2"],
                    "name": "int_model_2",
                    "original_file_path": "intermediate/int_model_2.sql",
                    "path": "intermediate/int_model_2.sql",
                    "unique_id": "model.package_name.int_model_2",
                },
                check_fails,
                id="has_source_dependency",
            ),
        ],
    )
    def test_check_lineage_source_cannot_be_used(self, model_overrides, check_fn):
        check_fn(
            "check_lineage_source_cannot_be_used",
            model=model_overrides,
        )
