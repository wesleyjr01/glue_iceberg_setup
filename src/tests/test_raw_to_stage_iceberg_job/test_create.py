import pytest
from glue_jobs.iceberg_job import RawToStageIcebergJob
from unittest.mock import patch


@pytest.mark.parametrize(
    "partitions,partition_by_statement",
    [
        ("year", "PARTITIONED BY (year)"),
        ("year, month", "PARTITIONED BY (year, month)"),
        ("NONE", ""),
    ],
)
def test_partition_by_statement(partitions, partition_by_statement):
    iceberg_job = RawToStageIcebergJob(
        spark=None,
        glue_context=None,
        job=None,
        iceberg_db="iceberg_db",
        iceberg_table="iceberg_table",
        source_db="source_db",
        source_table="source_table",
        partition_by=partitions,
        iceberg_table_update_mode="append",
    )
    assert iceberg_job.partition_by == partitions
    assert iceberg_job._partition_by_statement == partition_by_statement


def test_get_iceberg_table_create_query_no_partitions():
    iceberg_job = RawToStageIcebergJob(
        spark=None,
        glue_context=None,
        job=None,
        iceberg_db="iceberg_db",
        iceberg_table="iceberg_table",
        source_db="source_db",
        source_table="source_table",
        partition_by="NONE",
        iceberg_table_update_mode="append",
    )
    assert "PARTITIONED BY" not in iceberg_job.get_iceberg_table_create_query()
    assert (
        """
        CREATE TABLE glue_catalog.iceberg_db.iceberg_table
        USING iceberg
        
        TBLPROPERTIES ("format-version"="2")
        AS SELECT * FROM tmp_source_table
        """.strip()
        == iceberg_job.get_iceberg_table_create_query().strip()
    )


def test_get_iceberg_table_create_query_with_partitions():
    iceberg_job = RawToStageIcebergJob(
        spark=None,
        glue_context=None,
        job=None,
        iceberg_db="iceberg_db",
        iceberg_table="iceberg_table",
        source_db="source_db",
        source_table="source_table",
        partition_by="org, ingested_date",
        iceberg_table_update_mode="append",
    )
    assert "PARTITIONED BY" in iceberg_job.get_iceberg_table_create_query()
    assert (
        """
        CREATE TABLE glue_catalog.iceberg_db.iceberg_table
        USING iceberg
        PARTITIONED BY (org, ingested_date)
        TBLPROPERTIES ("format-version"="2")
        AS SELECT * FROM tmp_source_table
        """.strip()
        == iceberg_job.get_iceberg_table_create_query().strip()
    )


@pytest.mark.usefixtures("request")
@patch("glue_jobs.iceberg_job.RawToStageIcebergJob.read_source_table")
def test_create_iceberg_table(
    p_read_source_table,
    request,
    fixture_spark_session,
    fixture_df_increment_no_duplicates_inserts_only,
):
    p_read_source_table.return_value = fixture_df_increment_no_duplicates_inserts_only

    iceberg_db = "iceberg_db"
    test_name = request.node.name
    iceberg_table = test_name
    full_table_path = f"local.{iceberg_db}.{iceberg_table}"
    iceberg_job = RawToStageIcebergJob(
        spark=fixture_spark_session,
        glue_context=None,
        job=None,
        iceberg_db=iceberg_db,
        iceberg_table=iceberg_table,
        source_db="source_db",
        source_table="source_table",
        partition_by="ingested_date",
        iceberg_table_update_mode="append",
        catalog="local",
    )

    iceberg_job.create_iceberg_table(fixture_df_increment_no_duplicates_inserts_only)

    # assert number of records in created table == 2
    assert iceberg_job.spark.sql(f"SELECT COUNT(*) FROM {full_table_path}").collect()[0][0] == 2

    # assert the table has 4 fields
    df = iceberg_job.spark.read.format("iceberg").load(full_table_path)
    assert len(df.columns) == 4
