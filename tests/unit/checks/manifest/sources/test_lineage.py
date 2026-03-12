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


class TestCheckSourceNotOrphaned:
    def test_one_model_depends(self):
        check_passes(
            "check_source_not_orphaned",
            source=_SOURCE_WITH_TAGS,
            ctx_models=[_MODEL_DEPENDS_ON_SOURCE],
        )

    def test_two_models_depend(self):
        check_passes(
            "check_source_not_orphaned",
            source=_SOURCE_WITH_TAGS,
            ctx_models=[_MODEL_DEPENDS_ON_SOURCE, _MODEL_2_DEPENDS_ON_SOURCE],
        )

    def test_no_model_depends(self):
        check_fails(
            "check_source_not_orphaned",
            source=_SOURCE_WITH_TAGS,
            ctx_models=[_MODEL_NO_DEPS],
        )


class TestCheckSourceUsedByModelsInSameDirectory:
    def test_same_directory(self):
        check_passes(
            "check_source_used_by_models_in_same_directory",
            source=_SOURCE_WITH_TAGS,
            ctx_models=[
                {
                    **_MODEL_2_DEPENDS_ON_SOURCE,
                    "original_file_path": "models/staging/model_2.sql",
                    "path": "staging/model_2.sql",
                },
            ],
        )

    def test_different_directory(self):
        check_fails(
            "check_source_used_by_models_in_same_directory",
            source={
                "original_file_path": "models/_sources.yml",
                "path": "models/_sources.yml",
                "tags": ["tag_1"],
            },
            ctx_models=[
                {
                    **_MODEL_2_DEPENDS_ON_SOURCE,
                    "original_file_path": "models/staging/model_2.sql",
                    "path": "staging/model_2.sql",
                },
            ],
        )


class TestCheckSourceUsedByOnlyOneModel:
    def test_one_model_depends(self):
        check_passes(
            "check_source_used_by_only_one_model",
            source={
                "original_file_path": "models/_sources.yml",
                "path": "models/_sources.yml",
                "tags": ["tag_1"],
            },
            ctx_models=[
                {
                    **_MODEL_2_DEPENDS_ON_SOURCE,
                    "original_file_path": "models/staging/model_2.sql",
                    "path": "staging/model_2.sql",
                },
            ],
        )

    def test_no_model_depends(self):
        check_passes(
            "check_source_used_by_only_one_model",
            source={
                "original_file_path": "models/_sources.yml",
                "path": "models/_sources.yml",
                "tags": ["tag_1"],
            },
            ctx_models=[
                {
                    **_MODEL_NO_DEPS,
                    "original_file_path": "models/staging/model_2.sql",
                    "path": "staging/model_2.sql",
                },
            ],
        )

    def test_two_models_depend(self):
        check_fails(
            "check_source_used_by_only_one_model",
            source={
                "original_file_path": "models/_sources.yml",
                "path": "models/_sources.yml",
                "tags": ["tag_1"],
            },
            ctx_models=[
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
        )
