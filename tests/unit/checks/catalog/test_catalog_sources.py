import pytest

from dbt_bouncer.testing import check_fails, check_passes

_SOURCE_CATALOG_NODE = {
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
    "metadata": {
        "name": "table_1",
        "schema": "main",
        "type": "VIEW",
    },
    "unique_id": "source.package_name.source_1.table_1",
}


class TestCheckSourceColumnsAreAllDocumented:
    @pytest.mark.parametrize(
        ("catalog_source", "ctx_sources", "check_fn"),
        [
            pytest.param(
                _SOURCE_CATALOG_NODE,
                [
                    {
                        "columns": {
                            "col_1": {"name": "col_1"},
                            "col_2": {"name": "col_2"},
                        },
                        "fqn": ["package_name", "source_1", "table_1"],
                        "identifier": "table_1",
                        "loader": "csv",
                        "name": "table_1",
                        "original_file_path": "path/to/source_1.yml",
                        "path": "path/to/source_1.yml",
                        "source_description": "",
                        "source_name": "source_1",
                        "unique_id": "source.package_name.source_1.table_1",
                    }
                ],
                check_passes,
                id="all_documented",
            ),
            pytest.param(
                _SOURCE_CATALOG_NODE,
                [
                    {
                        "columns": {
                            "col_1": {"name": "col_1"},
                        },
                        "fqn": ["package_name", "source_1", "table_1"],
                        "identifier": "table_1",
                        "loader": "csv",
                        "name": "table_1",
                        "original_file_path": "path/to/source_1.yml",
                        "path": "path/to/source_1.yml",
                        "source_description": "",
                        "source_name": "source_1",
                        "unique_id": "source.package_name.source_1.table_1",
                    }
                ],
                check_fails,
                id="missing_documentation",
            ),
            pytest.param(
                {**_SOURCE_CATALOG_NODE, "columns": {}},
                [
                    {
                        "columns": {},
                        "fqn": ["package_name", "source_1", "table_1"],
                        "identifier": "table_1",
                        "loader": "csv",
                        "name": "table_1",
                        "original_file_path": "path/to/source_1.yml",
                        "path": "path/to/source_1.yml",
                        "source_description": "",
                        "source_name": "source_1",
                        "unique_id": "source.package_name.source_1.table_1",
                    }
                ],
                check_passes,
                id="no_columns_vacuously_passes",
            ),
            pytest.param(
                {
                    **_SOURCE_CATALOG_NODE,
                    "columns": {
                        "COL_1": {
                            **_SOURCE_CATALOG_NODE["columns"]["col_1"],
                            "name": "COL_1",
                        },
                        "COL_2": {
                            **_SOURCE_CATALOG_NODE["columns"]["col_2"],
                            "name": "COL_2",
                        },
                    },
                },
                [
                    {
                        "columns": {
                            "col_1": {"name": "col_1"},
                            "col_2": {"name": "col_2"},
                        },
                        "fqn": ["package_name", "source_1", "table_1"],
                        "identifier": "table_1",
                        "loader": "csv",
                        "name": "table_1",
                        "original_file_path": "path/to/source_1.yml",
                        "path": "path/to/source_1.yml",
                        "source_description": "",
                        "source_name": "source_1",
                        "unique_id": "source.package_name.source_1.table_1",
                    }
                ],
                check_fails,
                id="case_mismatch",
            ),
        ],
    )
    def test_check_source_columns_are_all_documented(
        self, catalog_source, ctx_sources, check_fn
    ):
        check_fn(
            "check_source_columns_are_all_documented",
            catalog_source=catalog_source,
            ctx_sources=ctx_sources,
        )


class TestCheckSourceColumnsAreAllDocumentedSnowflake:
    def test_uppercase_catalog_columns_pass_on_snowflake(self):
        check_passes(
            "check_source_columns_are_all_documented",
            catalog_source={
                **_SOURCE_CATALOG_NODE,
                "columns": {
                    "COL_1": {
                        **_SOURCE_CATALOG_NODE["columns"]["col_1"],
                        "name": "COL_1",
                    },
                    "COL_2": {
                        **_SOURCE_CATALOG_NODE["columns"]["col_2"],
                        "name": "COL_2",
                    },
                },
            },
            ctx_sources=[
                {
                    "columns": {
                        "col_1": {"name": "col_1"},
                        "col_2": {"name": "col_2"},
                    },
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "csv",
                    "name": "table_1",
                    "original_file_path": "path/to/source_1.yml",
                    "path": "path/to/source_1.yml",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ],
            ctx_manifest_obj={"metadata": {"adapter_type": "snowflake"}},
        )

    def test_case_insensitive_opt_in_on_non_snowflake_adapter(self):
        # A non-Snowflake adapter keeps the default postgres manifest, so the
        # case-insensitive path is only reached via an explicit parameter.
        check_passes(
            "check_source_columns_are_all_documented",
            case_sensitive=False,
            catalog_source={
                **_SOURCE_CATALOG_NODE,
                "columns": {
                    "COL_1": {
                        **_SOURCE_CATALOG_NODE["columns"]["col_1"],
                        "name": "COL_1",
                    },
                    "COL_2": {
                        **_SOURCE_CATALOG_NODE["columns"]["col_2"],
                        "name": "COL_2",
                    },
                },
            },
            ctx_sources=[
                {
                    "columns": {
                        "col_1": {"name": "col_1"},
                        "col_2": {"name": "col_2"},
                    },
                    "fqn": ["package_name", "source_1", "table_1"],
                    "identifier": "table_1",
                    "loader": "csv",
                    "name": "table_1",
                    "original_file_path": "path/to/source_1.yml",
                    "path": "path/to/source_1.yml",
                    "source_description": "",
                    "source_name": "source_1",
                    "unique_id": "source.package_name.source_1.table_1",
                }
            ],
        )

    def test_check_source_columns_are_all_documented_source_missing_from_manifest(
        self,
    ):
        with pytest.raises(StopIteration):
            check_passes(
                "check_source_columns_are_all_documented",
                catalog_source={
                    "unique_id": "source.package_name.source_1.table_1",
                },
            )
