from glue_jobs.iceberg_job import RawToStageIcebergJob
import pytest


def test_if_append_mode_is_implemented():
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
    assert iceberg_job.iceberg_table_update_mode == "append"
    assert iceberg_job._iceberg_table_update_mode == "append"


def test_if_overwrite_mode_is_not_implemented():
    with pytest.raises(NotImplementedError):
        iceberg_job = RawToStageIcebergJob(
            spark=None,
            glue_context=None,
            job=None,
            iceberg_db="iceberg_db",
            iceberg_table="iceberg_table",
            source_db="source_db",
            source_table="source_table",
            partition_by="NONE",
            iceberg_table_update_mode="overwrite",
        )


def test_if_upsert_mode_is_not_implemented():
    with pytest.raises(NotImplementedError):
        iceberg_job = RawToStageIcebergJob(
            spark=None,
            glue_context=None,
            job=None,
            iceberg_db="iceberg_db",
            iceberg_table="iceberg_table",
            source_db="source_db",
            source_table="source_table",
            partition_by="NONE",
            iceberg_table_update_mode="upsert",
        )


def test_if_random_mode_raises_value_error():
    with pytest.raises(ValueError):
        iceberg_job = RawToStageIcebergJob(
            spark=None,
            glue_context=None,
            job=None,
            iceberg_db="iceberg_db",
            iceberg_table="iceberg_table",
            source_db="source_db",
            source_table="source_table",
            partition_by="NONE",
            iceberg_table_update_mode="rAndOm",
        )
