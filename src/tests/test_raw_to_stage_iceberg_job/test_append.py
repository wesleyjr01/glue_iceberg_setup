import pytest
from glue_jobs.iceberg_job import RawToStageIcebergJob
from unittest.mock import patch


@pytest.mark.usefixtures("request")
@patch("glue_jobs.iceberg_job.RawToStageIcebergJob.read_source_table")
def test_create_or_update_iceberg_table_create_then_append(
    p_read_source_table,
    request,
    fixture_spark_session,
    fixture_df_increment_no_duplicates_inserts_only,
):
    p_read_source_table.return_value = fixture_df_increment_no_duplicates_inserts_only

    iceberg_db = "iceberg_db"
    test_name = request.node.name
    iceberg_table = test_name
    iceberg_job = RawToStageIcebergJob(
        spark=fixture_spark_session,
        glue_context=None,
        iceberg_db=iceberg_db,
        iceberg_table=iceberg_table,
        source_db="source_db",
        source_table="source_table",
        partition_by="ingested_date",
        iceberg_table_update_mode="append",
        catalog="local",
    )

    ####### CREATE ICEBERG TABLE FROM EXISTING DATASET #######
    iceberg_job.create_iceberg_table(fixture_df_increment_no_duplicates_inserts_only)
    table_path = iceberg_job._iceberg_table_full_path

    # assert number of records in created table == 2
    assert iceberg_job.spark.sql(f"SELECT COUNT(*) FROM {table_path}").collect()[0][0] == 2

    # assert the table has 4 fields
    df = iceberg_job.spark.read.format("iceberg").load(table_path)
    assert len(df.columns) == 4
    ####### CREATE ICEBERG TABLE FROM EXISTING DATASET #######

    #### NOW LET'S APPEND THE SAME DATASET TO THE ICEBERG TABLE, RECORDS SHOULD DOUBLE ####
    iceberg_job.append_to_iceberg_table(fixture_df_increment_no_duplicates_inserts_only)

    # assert number of records in created table == 4
    assert iceberg_job.spark.sql(f"SELECT COUNT(*) FROM {table_path}").collect()[0][0] == 4

    # assert the table has 4 fields
    df = iceberg_job.spark.read.format("iceberg").load(table_path)
    assert len(df.columns) == 4
    #### NOW LET'S APPEND THE SAME DATASET TO THE ICEBERG TABLE, RECORDS SHOULD DOUBLE ####


@pytest.mark.usefixtures("request")
@patch("glue_jobs.iceberg_job.RawToStageIcebergJob.read_source_table")
def test_create_or_update_iceberg_table_create_then_append_twice(
    p_read_source_table,
    request,
    fixture_spark_session,
    fixture_df_increment_no_duplicates_inserts_only,
):
    p_read_source_table.return_value = fixture_df_increment_no_duplicates_inserts_only

    iceberg_db = "iceberg_db"
    test_name = request.node.name
    iceberg_table = test_name
    iceberg_job = RawToStageIcebergJob(
        spark=fixture_spark_session,
        glue_context=None,
        iceberg_db=iceberg_db,
        iceberg_table=iceberg_table,
        source_db="source_db",
        source_table="source_table",
        partition_by="ingested_date",
        iceberg_table_update_mode="append",
        catalog="local",
    )

    ####### CREATE ICEBERG TABLE FROM EXISTING DATASET #######
    iceberg_job.create_iceberg_table(fixture_df_increment_no_duplicates_inserts_only)
    table_path = iceberg_job._iceberg_table_full_path

    # assert number of records in created table == 2
    assert iceberg_job.spark.sql(f"SELECT COUNT(*) FROM {table_path}").collect()[0][0] == 2

    # assert the table has 4 fields
    df = iceberg_job.spark.read.format("iceberg").load(table_path)
    assert len(df.columns) == 4
    ####### CREATE ICEBERG TABLE FROM EXISTING DATASET #######

    #### NOW LET'S APPEND THE SAME DATASET TO THE ICEBERG TABLE, RECORDS SHOULD DOUBLE ####
    iceberg_job.append_to_iceberg_table(fixture_df_increment_no_duplicates_inserts_only)

    # assert number of records in created table == 4
    assert iceberg_job.spark.sql(f"SELECT COUNT(*) FROM {table_path}").collect()[0][0] == 4

    # assert the table has 4 fields
    df = iceberg_job.spark.read.format("iceberg").load(table_path)
    assert len(df.columns) == 4
    #### NOW LET'S APPEND THE SAME DATASET TO THE ICEBERG TABLE, RECORDS SHOULD DOUBLE ####

    #### A SECOND TIME, LET'S APPEND THE SAME DATASET TO THE ICEBERG TABLE, RECORDS SHOULD TRIPLE ####
    iceberg_job.append_to_iceberg_table(fixture_df_increment_no_duplicates_inserts_only)

    # assert number of records in created table == 6
    assert iceberg_job.spark.sql(f"SELECT COUNT(*) FROM {table_path}").collect()[0][0] == 6

    # assert the table has 4 fields
    df = iceberg_job.spark.read.format("iceberg").load(table_path)
    assert len(df.columns) == 4
    #### A SECOND TIME, LET'S APPEND THE SAME DATASET TO THE ICEBERG TABLE, RECORDS SHOULD TRIPLE ####
