import duckdb
import pytest

from dbt_bouncer.artifact_parsers.info_schema import InfoSchema, info_schema_dir
from dbt_bouncer.exceptions import DbtBouncerArtifactError
from tests.unit.checks.info_schema.conftest import write_info_schema


def test_info_schema_dir_is_versioned(tmp_path):
    assert info_schema_dir(tmp_path) == tmp_path / "info_schema" / "v1"


def test_missing_directory_raises(tmp_path):
    with pytest.raises(DbtBouncerArtifactError, match="--generate-info-schema"):
        InfoSchema.from_directory(tmp_path / "info_schema" / "v1")


def test_unsupported_schema_version_raises(tmp_path):
    directory = write_info_schema(tmp_path / "v1", schema_version=2)
    with pytest.raises(DbtBouncerArtifactError, match="version 1 only"):
        InfoSchema.from_directory(directory)


def test_tables_and_indexes(info_schema_dir):
    info_schema = InfoSchema.from_directory(info_schema_dir)

    assert info_schema.models_by_unique_id["model.demo.orders"]["grain"] == ["order_id"]
    assert info_schema.table("dbt.does_not_exist") == []
    # Time zone aware timestamps are returned as text (no `pytz` needed).
    assert isinstance(info_schema.table("dbt.project")[0]["ingested_at"], str)
    edges = info_schema.lineage_by_child["model.demo.orders"]["order_id"]
    assert [e.parent_column_name for e in edges] == ["id"]
    assert info_schema.lineage_parent_columns == {"source.demo.raw.orders": {"id"}}


def test_query_returns_text(info_schema_dir):
    columns, rows = InfoSchema.from_directory(info_schema_dir).query(
        "SELECT name, grain FROM dbt.models ORDER BY name;"
    )

    assert columns == ["name", "grain"]
    assert rows == [("customers", "[]"), ("orders", "[order_id]")]


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("SELECT * FROM read_csv('{path}')", id="read_file"),
        pytest.param("SELECT * FROM '{path}'", id="replacement_scan"),
    ],
)
def test_query_cannot_read_files(info_schema_dir, sql):
    info_schema = InfoSchema.from_directory(info_schema_dir)
    path = (info_schema_dir / "dbt.models.parquet").as_posix()

    with pytest.raises(duckdb.Error):
        info_schema.query(sql.format(path=path))


def test_in_memory_rows_have_no_database():
    with pytest.raises(DbtBouncerArtifactError, match="no database"):
        InfoSchema.from_rows(models=[]).query("SELECT 1")
