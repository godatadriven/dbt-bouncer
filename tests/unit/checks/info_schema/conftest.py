import duckdb
import pytest


def write_info_schema(directory, schema_version=1):
    """Write a minimal dbt Information Schema (`v1`) into `directory`.

    Returns:
        Path: The directory.

    """
    directory.mkdir(parents=True, exist_ok=True)
    tables = {
        "dbt.project": f"SELECT {schema_version} AS schema_version, 'demo' AS project_name, TIMESTAMPTZ '2026-10-07 10:00:00+00' AS ingested_at",
        "dbt.models": """
            SELECT * FROM (VALUES
                ('model.demo.orders', 'orders', 'public', 'Orders.', ['order_id']),
                ('model.demo.customers', 'customers', 'public', NULL, []::VARCHAR[])
            ) AS t(unique_id, name, access, description, grain)
        """,
        "dbt.column_lineage": """
            SELECT 'source.demo.raw.orders' AS parent_node_unique_id, 'id' AS parent_column_name,
                   'model.demo.orders' AS child_node_unique_id, 'order_id' AS child_column_name,
                   'copy' AS evolution
        """,
    }
    con = duckdb.connect()
    for name, sql in tables.items():
        target = (directory / f"{name}.parquet").as_posix()
        con.execute(f"COPY ({sql}) TO '{target}' (FORMAT parquet)")
    con.close()
    return directory


@pytest.fixture
def info_schema_dir(tmp_path):
    return write_info_schema(tmp_path / "info_schema" / "v1")
