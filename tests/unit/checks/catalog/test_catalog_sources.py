from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceColumnsAreAllDocumented:
    def test_all_documented(self):
        check_passes(
            "check_source_columns_are_all_documented",
            catalog_source={
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

    def test_missing_documentation(self):
        check_fails(
            "check_source_columns_are_all_documented",
            catalog_source={
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
            },
            ctx_sources=[
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
        )
