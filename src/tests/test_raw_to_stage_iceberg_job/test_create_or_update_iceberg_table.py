import pytest
from glue_jobs.iceberg_job import RawToStageIcebergJob
from unittest.mock import patch


@pytest.mark.usefixtures("request")
@patch("glue_jobs.iceberg_job.RawToStageIcebergJob.check_if_iceberg_table_exists")
@patch("glue_jobs.iceberg_job.RawToStageIcebergJob.read_source_table")
def test_create_or_update_iceberg_table_method_created_a_table(
    p_read_source_table,
    p_check_if_iceberg_table_exists,
    request,
    fixture_spark_session,
    fixture_df_increment_no_duplicates_inserts_only,
):
    p_read_source_table.return_value = fixture_df_increment_no_duplicates_inserts_only
    p_check_if_iceberg_table_exists.return_value = False

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

    iceberg_job.create_or_update_iceberg_table()
    table_path = iceberg_job._iceberg_table_full_path

    # assert called method create_iceberg_table() once
    assert p_read_source_table.call_count == 1

    # assert number of records in created table == 2
    assert iceberg_job.spark.sql(f"SELECT COUNT(*) FROM {table_path}").collect()[0][0] == 2

    # assert the table has 4 fields
    df = iceberg_job.spark.read.format("iceberg").load(table_path)
    assert len(df.columns) == 4


@pytest.mark.usefixtures("request")
@patch("glue_jobs.iceberg_job.RawToStageIcebergJob.check_if_iceberg_table_exists")
@patch("glue_jobs.iceberg_job.RawToStageIcebergJob.read_source_table")
def test_create_or_update_iceberg_table_method_created_a_then_appended_table(
    p_read_source_table,
    p_check_if_iceberg_table_exists,
    request,
    fixture_spark_session,
    fixture_df_increment_no_duplicates_inserts_only,
):
    p_read_source_table.return_value = fixture_df_increment_no_duplicates_inserts_only
    p_check_if_iceberg_table_exists.side_effect = [False, True]

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

    ################ CREATE ################
    iceberg_job.create_or_update_iceberg_table()
    table_path = iceberg_job._iceberg_table_full_path

    # assert called method create_iceberg_table() once
    assert p_read_source_table.call_count == 1

    # assert number of records in created table == 2
    assert iceberg_job.spark.sql(f"SELECT COUNT(*) FROM {table_path}").collect()[0][0] == 2

    # assert the table has 4 fields
    df = iceberg_job.spark.read.format("iceberg").load(table_path)
    assert len(df.columns) == 4
    ################ CREATE ################

    ################ APPEND ################
    iceberg_job.create_or_update_iceberg_table()
    table_path = iceberg_job._iceberg_table_full_path

    # assert called method create_iceberg_table() twice
    assert p_read_source_table.call_count == 2

    # assert number of records in created table == 4
    assert iceberg_job.spark.sql(f"SELECT COUNT(*) FROM {table_path}").collect()[0][0] == 4

    # assert the table has 4 fields
    df = iceberg_job.spark.read.format("iceberg").load(table_path)
    assert len(df.columns) == 4
    ################ APPEND ################


@pytest.mark.usefixtures("request")
@patch("glue_jobs.iceberg_job.RawToStageIcebergJob.check_if_iceberg_table_exists")
@patch("glue_jobs.iceberg_job.RawToStageIcebergJob.read_source_table")
def test_create_or_update_iceberg_table_method_created_a_then_cannot_append_to_table_because_no_new_records(
    p_read_source_table,
    p_check_if_iceberg_table_exists,
    request,
    fixture_spark_session,
    fixture_df_increment_no_duplicates_inserts_only,
    fixture_df_empty,
):
    p_read_source_table.side_effect = [fixture_df_increment_no_duplicates_inserts_only, fixture_df_empty]
    p_check_if_iceberg_table_exists.side_effect = [False, True]

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

    ################ CREATE ################
    iceberg_job.create_or_update_iceberg_table()
    table_path = iceberg_job._iceberg_table_full_path

    # assert called method create_iceberg_table() once
    assert p_read_source_table.call_count == 1

    # assert number of records in created table == 2
    assert iceberg_job.spark.sql(f"SELECT COUNT(*) FROM {table_path}").collect()[0][0] == 2

    # assert the table has 4 fields
    df = iceberg_job.spark.read.format("iceberg").load(table_path)
    assert len(df.columns) == 4
    ################ CREATE ################

    ################ APPEND ################
    iceberg_job.create_or_update_iceberg_table()
    table_path = iceberg_job._iceberg_table_full_path

    # assert check_if_iceberg_table_exists called only once, since the second time there was no records to append
    assert p_check_if_iceberg_table_exists.call_count == 1

    # assert called method create_iceberg_table() twice
    assert p_read_source_table.call_count == 2

    # assert number of records in created table == 4
    assert iceberg_job.spark.sql(f"SELECT COUNT(*) FROM {table_path}").collect()[0][0] == 2

    # assert the table has 4 fields
    df = iceberg_job.spark.read.format("iceberg").load(table_path)
    assert len(df.columns) == 4
    ################ APPEND ################
