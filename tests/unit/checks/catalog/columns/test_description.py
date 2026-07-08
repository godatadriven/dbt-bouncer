import pytest

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
    @pytest.mark.parametrize(
        ("catalog_node", "ctx_models", "check_fn"),
        [
            pytest.param(
                {},
                [_MODEL_WITH_DESCRIPTIONS],
                check_passes,
                id="all_documented",
            ),
            pytest.param(
                {"unique_id": "model.package_name.model_2"},
                [
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
                check_fails,
                id="missing_documentation",
            ),
        ],
    )
    def test_check_column_description_populated(
        self, catalog_node, ctx_models, check_fn
    ):
        check_fn(
            "check_column_description_populated",
            catalog_node=catalog_node,
            ctx_models=ctx_models,
        )


class TestCheckColumnDescriptionPopulatedSnowflake:
    @pytest.mark.parametrize(
        ("catalog_node", "check_fn"),
        [
            pytest.param(
                {
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
                check_passes,
                id="all_documented_snowflake",
            ),
            pytest.param(
                {
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
                check_fails,
                id="missing_documentation_snowflake",
            ),
        ],
    )
    def test_check_column_description_populated(self, catalog_node, check_fn):
        check_fn(
            "check_column_description_populated",
            catalog_node=catalog_node,
            ctx_models=[_MODEL_WITH_DESCRIPTIONS],
            ctx_manifest_obj={"metadata": {"adapter_type": "snowflake"}},
        )


class TestCheckColumnsAreAllDocumented:
    @pytest.mark.parametrize(
        ("catalog_node", "ctx_models", "check_fn"),
        [
            pytest.param(
                {},
                [_MODEL_WITH_DESCRIPTIONS],
                check_passes,
                id="all_documented",
            ),
            pytest.param(
                {
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
                [_MODEL_WITH_DESCRIPTIONS],
                check_fails,
                id="case_mismatch",
            ),
            pytest.param(
                {"unique_id": "model.package_name.model_2"},
                [
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
                check_fails,
                id="missing_column_in_model",
            ),
        ],
    )
    def test_check_columns_are_all_documented(self, catalog_node, ctx_models, check_fn):
        check_fn(
            "check_columns_are_all_documented",
            catalog_node=catalog_node,
            ctx_models=ctx_models,
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
    @pytest.mark.parametrize(
        ("catalog_node", "ctx_models", "check_fn"),
        [
            pytest.param(
                {
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                },
                [
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
                check_passes,
                id="documented_public",
            ),
            pytest.param(
                {},
                [
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
                check_fails,
                id="undocumented_public",
            ),
        ],
    )
    def test_check_columns_are_documented_in_public_models(
        self, catalog_node, ctx_models, check_fn
    ):
        check_fn(
            "check_columns_are_documented_in_public_models",
            catalog_node=catalog_node,
            ctx_models=ctx_models,
        )
