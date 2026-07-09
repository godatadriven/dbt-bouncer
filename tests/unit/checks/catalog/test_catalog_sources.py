import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceColumnsAreAllDocumented:
    @pytest.mark.parametrize(
        ("catalog_source", "ctx_sources", "check_fn"),
        [
            pytest.param(
                {
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
                {
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
                {
                    "columns": {},
                    "metadata": {
                        "name": "table_1",
                        "schema": "main",
                        "type": "VIEW",
                    },
                    "unique_id": "source.package_name.source_1.table_1",
                },
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
