from dbt_bouncer.testing import check_fails, check_passes

# Default model with documented columns (matches old conftest fixture)
_MODEL_WITH_DESCRIPTIONS = {
    "columns": {
        "col_1": {
            "description": "This is a description",
            "index": 1,
            "name": "col_1",
            "type": "INTEGER",
        },
        "col_2": {
            "description": "This is a description",
            "index": 2,
            "name": "col_2",
            "type": "INTEGER",
        },
    },
}


class TestCheckColumnDescriptionPopulated:
    def test_all_documented(self):
        check_passes(
            "check_column_description_populated",
            catalog_node={},
            ctx_models=[_MODEL_WITH_DESCRIPTIONS],
        )

    def test_missing_documentation(self):
        check_fails(
            "check_column_description_populated",
            catalog_node={"unique_id": "model.package_name.model_2"},
            ctx_models=[
                {
                    "alias": "model_2",
                    "columns": {
                        "col_1": {
                            "description": "This is a description",
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
                    "path": "model_2.sql",
                    "unique_id": "model.package_name.model_2",
                }
            ],
        )


class TestCheckColumnDescriptionPopulatedSnowflake:
    def test_all_documented_snowflake(self):
        check_passes(
            "check_column_description_populated",
            catalog_node={
                "columns": {
                    "col_1": {
                        "index": 1,
                        "name": "col_1",
                        "type": "INTEGER",
                        "comment": "This is a description",
                    },
                    "col_2": {
                        "index": 2,
                        "name": "col_2",
                        "type": "INTEGER",
                        "comment": "This is a description",
                    },
                },
            },
            ctx_models=[_MODEL_WITH_DESCRIPTIONS],
            ctx_manifest_obj={"metadata": {"adapter_type": "snowflake"}},
        )

    def test_missing_documentation_snowflake(self):
        check_fails(
            "check_column_description_populated",
            catalog_node={
                "columns": {
                    "col_1": {
                        "index": 1,
                        "name": "col_1",
                        "type": "INTEGER",
                        "comment": "This is a description",
                    },
                    "col_2": {
                        "index": 2,
                        "name": "col_2",
                        "type": "INTEGER",
                    },
                },
            },
            ctx_models=[_MODEL_WITH_DESCRIPTIONS],
            ctx_manifest_obj={"metadata": {"adapter_type": "snowflake"}},
        )


class TestCheckColumnsAreAllDocumented:
    def test_all_documented(self):
        check_passes(
            "check_columns_are_all_documented",
            catalog_node={},
            ctx_models=[_MODEL_WITH_DESCRIPTIONS],
        )

    def test_case_mismatch(self):
        check_fails(
            "check_columns_are_all_documented",
            catalog_node={
                "columns": {
                    "COL_1": {
                        "index": 1,
                        "name": "COL_1",
                        "type": "INTEGER",
                    },
                    "COL_2": {
                        "index": 2,
                        "name": "COL_2",
                        "type": "INTEGER",
                    },
                },
            },
            ctx_models=[_MODEL_WITH_DESCRIPTIONS],
        )

    def test_missing_column_in_model(self):
        check_fails(
            "check_columns_are_all_documented",
            catalog_node={"unique_id": "model.package_name.model_2"},
            ctx_models=[
                {
                    "alias": "model_2",
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "fqn": ["package_name", "model_2"],
                    "name": "model_2",
                    "original_file_path": "model_2.sql",
                    "path": "model_2.sql",
                    "unique_id": "model.package_name.model_2",
                }
            ],
        )


class TestCheckColumnsAreAllDocumentedSnowflake:
    def test_all_documented_snowflake(self):
        check_passes(
            "check_columns_are_all_documented",
            catalog_node={
                "columns": {
                    "COL_1": {
                        "index": 1,
                        "name": "COL_1",
                        "type": "INTEGER",
                    },
                    "COL_2": {
                        "index": 2,
                        "name": "COL_2",
                        "type": "INTEGER",
                    },
                },
            },
            ctx_models=[_MODEL_WITH_DESCRIPTIONS],
            ctx_manifest_obj={"metadata": {"adapter_type": "snowflake"}},
        )


class TestCheckColumnsAreDocumentedInPublicModels:
    def test_documented_public(self):
        check_passes(
            "check_columns_are_documented_in_public_models",
            catalog_node={
                "columns": {
                    "col_1": {
                        "index": 1,
                        "name": "col_1",
                        "type": "INTEGER",
                    },
                },
            },
            ctx_models=[
                {
                    "access": "public",
                    "columns": {
                        "col_1": {
                            "description": "This is a description",
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                }
            ],
        )

    def test_undocumented_public(self):
        check_fails(
            "check_columns_are_documented_in_public_models",
            catalog_node={},
            ctx_models=[
                {
                    "access": "public",
                    "columns": {
                        "col_1": {
                            "description": "This is a description",
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                        "col_2": {
                            "description": "",
                            "index": 2,
                            "name": "col_2",
                            "type": "INTEGER",
                        },
                    },
                }
            ],
        )
