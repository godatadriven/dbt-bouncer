from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSeedColumnsAreAllDocumented:
    def test_all_columns_documented(self):
        check_passes(
            "check_seed_columns_are_all_documented",
            catalog_node={
                "columns": {
                    "id": {"name": "id", "type": "INTEGER", "index": 1},
                    "first_name": {
                        "name": "first_name",
                        "type": "VARCHAR",
                        "index": 2,
                    },
                    "last_name": {
                        "name": "last_name",
                        "type": "VARCHAR",
                        "index": 3,
                    },
                },
                "metadata": {
                    "database": "dbt",
                    "name": "raw_customers",
                    "schema": "main",
                    "type": "BASE TABLE",
                },
                "unique_id": "seed.package_name.raw_customers",
            },
            ctx_seeds=[
                {
                    "alias": "raw_customers",
                    "columns": {
                        "id": {"name": "id"},
                        "first_name": {"name": "first_name"},
                        "last_name": {"name": "last_name"},
                    },
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "path": "raw_customers.csv",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ],
        )

    def test_missing_last_name_column(self):
        check_fails(
            "check_seed_columns_are_all_documented",
            catalog_node={
                "columns": {
                    "id": {"name": "id", "type": "INTEGER", "index": 1},
                    "first_name": {
                        "name": "first_name",
                        "type": "VARCHAR",
                        "index": 2,
                    },
                    "last_name": {
                        "name": "last_name",
                        "type": "VARCHAR",
                        "index": 3,
                    },
                },
                "metadata": {
                    "database": "dbt",
                    "name": "raw_customers",
                    "schema": "main",
                    "type": "BASE TABLE",
                },
                "unique_id": "seed.package_name.raw_customers",
            },
            ctx_seeds=[
                {
                    "alias": "raw_customers",
                    "columns": {
                        "id": {"name": "id"},
                        "first_name": {"name": "first_name"},
                    },
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.csv",
                    "path": "raw_customers.csv",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ],
        )
