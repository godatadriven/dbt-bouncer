import pytest

from dbt_bouncer.testing import check_fails, check_passes

# Shared model dicts for reuse across tests
_MODEL_DEPENDS_ON_SOURCE = {
    "alias": "model_1",
    "checksum": {"name": "sha256", "checksum": ""},
    "columns": {
        "col_1": {"index": 1, "name": "col_1", "type": "INTEGER"},
    },
    "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
    "fqn": ["package_name", "model_1"],
    "name": "model_1",
    "original_file_path": "model_1.sql",
    "package_name": "package_name",
    "path": "model_1.sql",
    "resource_type": "model",
    "schema": "main",
    "unique_id": "model.package_name.model_1",
}

_MODEL_2_DEPENDS_ON_SOURCE = {
    "alias": "model_2",
    "checksum": {"name": "sha256", "checksum": ""},
    "columns": {
        "col_1": {"index": 1, "name": "col_1", "type": "INTEGER"},
    },
    "depends_on": {"nodes": ["source.package_name.source_1.table_1"]},
    "fqn": ["package_name", "model_2"],
    "name": "model_2",
    "original_file_path": "model_2.sql",
    "package_name": "package_name",
    "path": "model_2.sql",
    "resource_type": "model",
    "schema": "main",
    "unique_id": "model.package_name.model_2",
}

_MODEL_NO_DEPS = {
    "alias": "model_2",
    "checksum": {"name": "sha256", "checksum": ""},
    "columns": {
        "col_1": {"index": 1, "name": "col_1", "type": "INTEGER"},
    },
    "depends_on": {"nodes": []},
    "fqn": ["package_name", "model_2"],
    "name": "model_2",
    "original_file_path": "model_2.sql",
    "package_name": "package_name",
    "path": "model_2.sql",
    "resource_type": "model",
    "schema": "main",
    "unique_id": "model.package_name.model_2",
}

_SOURCE_WITH_TAGS = {
    "tags": ["tag_1"],
}

# Source dicts used in check_duplicate_sources tests.
_SOURCE_A = {
    "database": "dev",
    "identifier": "table_1",
    "schema": "schema_1",
    "unique_id": "source.package_name.source_1.table_1",
}

_SOURCE_B_DUPLICATE = {
    "database": "dev",
    "identifier": "table_1",
    "schema": "schema_1",
    "unique_id": "source.package_name.source_2.table_1",  # different unique_id, same relation
}

_SOURCE_C_UNIQUE = {
    "database": "dev",
    "identifier": "table_2",
    "schema": "schema_1",
    "unique_id": "source.package_name.source_1.table_2",  # different identifier
}


class TestCheckDuplicateSources:
    def test_all_relations_unique(self):
        check_passes(
            "check_duplicate_sources",
            source=_SOURCE_A,
            ctx_sources=[_SOURCE_A, _SOURCE_C_UNIQUE],
        )

    def test_duplicate_relation(self):
        check_fails(
            "check_duplicate_sources",
            source=_SOURCE_A,
            ctx_sources=[_SOURCE_A, _SOURCE_B_DUPLICATE],
        )


class TestCheckSourceMinDownstreamModels:
    @pytest.mark.parametrize(
        ("min_models", "models_list"),
        [
            pytest.param(
                1,
                [_MODEL_DEPENDS_ON_SOURCE],
                id="exactly_min",
            ),
            pytest.param(
                1,
                [_MODEL_DEPENDS_ON_SOURCE, _MODEL_2_DEPENDS_ON_SOURCE],
                id="above_min",
            ),
            pytest.param(
                2,
                [_MODEL_DEPENDS_ON_SOURCE, _MODEL_2_DEPENDS_ON_SOURCE],
                id="two_models_min_two",
            ),
        ],
    )
    def test_pass(self, min_models, models_list):
        check_passes(
            "check_source_min_downstream_models",
            source=_SOURCE_WITH_TAGS,
            min_number_of_models=min_models,
            ctx_models=models_list,
        )

    @pytest.mark.parametrize(
        ("min_models", "models_list"),
        [
            pytest.param(
                2,
                [_MODEL_DEPENDS_ON_SOURCE],
                id="below_min_one_model",
            ),
            pytest.param(
                1,
                [_MODEL_NO_DEPS],
                id="below_min_no_matching_models",
            ),
        ],
    )
    def test_fail(self, min_models, models_list):
        check_fails(
            "check_source_min_downstream_models",
            source=_SOURCE_WITH_TAGS,
            min_number_of_models=min_models,
            ctx_models=models_list,
        )

    def test_invalid_min_raises_value_error(self):
        from dbt_bouncer.testing import _run_check

        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_source_min_downstream_models",
                source=_SOURCE_WITH_TAGS,
                min_number_of_models=0,
                ctx_models=[_MODEL_DEPENDS_ON_SOURCE],
            )


class TestCheckSourceNotOrphaned:
    @pytest.mark.parametrize(
        ("ctx_models", "check_fn"),
        [
            pytest.param(
                [_MODEL_DEPENDS_ON_SOURCE], check_passes, id="one_model_depends"
            ),
            pytest.param(
                [_MODEL_DEPENDS_ON_SOURCE, _MODEL_2_DEPENDS_ON_SOURCE],
                check_passes,
                id="two_models_depend",
            ),
            pytest.param([_MODEL_NO_DEPS], check_fails, id="no_model_depends"),
        ],
    )
    def test_check_source_not_orphaned(self, ctx_models, check_fn):
        check_fn(
            "check_source_not_orphaned",
            source=_SOURCE_WITH_TAGS,
            ctx_models=ctx_models,
        )


class TestCheckSourceUsedByModelsInSameDirectory:
    @pytest.mark.parametrize(
        ("source", "ctx_models", "check_fn"),
        [
            pytest.param(
                _SOURCE_WITH_TAGS,
                [
                    {
                        **_MODEL_2_DEPENDS_ON_SOURCE,
                        "original_file_path": "models/staging/model_2.sql",
                        "path": "staging/model_2.sql",
                    },
                ],
                check_passes,
                id="same_directory",
            ),
            pytest.param(
                {
                    "original_file_path": "models/_sources.yml",
                    "path": "models/_sources.yml",
                    "tags": ["tag_1"],
                },
                [
                    {
                        **_MODEL_2_DEPENDS_ON_SOURCE,
                        "original_file_path": "models/staging/model_2.sql",
                        "path": "staging/model_2.sql",
                    },
                ],
                check_fails,
                id="different_directory",
            ),
        ],
    )
    def test_check_source_used_by_models_in_same_directory(
        self, source, ctx_models, check_fn
    ):
        check_fn(
            "check_source_used_by_models_in_same_directory",
            source=source,
            ctx_models=ctx_models,
        )


class TestCheckSourceUsedByOnlyOneModel:
    @pytest.mark.parametrize(
        ("ctx_models", "check_fn"),
        [
            pytest.param(
                [
                    {
                        **_MODEL_2_DEPENDS_ON_SOURCE,
                        "original_file_path": "models/staging/model_2.sql",
                        "path": "staging/model_2.sql",
                    },
                ],
                check_passes,
                id="one_model_depends",
            ),
            pytest.param(
                [
                    {
                        **_MODEL_NO_DEPS,
                        "original_file_path": "models/staging/model_2.sql",
                        "path": "staging/model_2.sql",
                    },
                ],
                check_passes,
                id="no_model_depends",
            ),
            pytest.param(
                [
                    {
                        **_MODEL_DEPENDS_ON_SOURCE,
                        "alias": "model_1",
                        "name": "model_1",
                        "original_file_path": "models/staging/model_1.sql",
                        "path": "staging/model_1.sql",
                        "unique_id": "model.package_name.model_1",
                    },
                    {
                        **_MODEL_2_DEPENDS_ON_SOURCE,
                        "original_file_path": "models/staging/model_2.sql",
                        "path": "staging/model_2.sql",
                    },
                ],
                check_fails,
                id="two_models_depend",
            ),
        ],
    )
    def test_check_source_used_by_only_one_model(self, ctx_models, check_fn):
        check_fn(
            "check_source_used_by_only_one_model",
            source={
                "original_file_path": "models/_sources.yml",
                "path": "models/_sources.yml",
                "tags": ["tag_1"],
            },
            ctx_models=ctx_models,
        )
