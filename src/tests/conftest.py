import pytest
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

iceberg_jar = "/opt/spark/iceberg-spark-runtime-3.3_2.12-1.0.0.jar"


@pytest.fixture(scope="session")
def fixture_spark_session():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    warehouse_path = f"file:///tmp/iceberg/{timestamp}"
    spark = (
        SparkSession.builder.appName("Iceberg with PySpark")
        .config("spark.jars", iceberg_jar)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
        .getOrCreate()
    )
    return spark


@pytest.fixture(scope="session")
def fixture_glue_context(fixture_spark_session):
    glue_context = GlueContext(fixture_spark_session)
    return glue_context


@pytest.fixture(scope="session")
def fixture_df_increment_no_duplicates_inserts_only(fixture_spark_session) -> DataFrame:
    data = [
        {
            "id": 1,
            "name": "John",
            "ingested_date": "2022-01-01",
            "op": "I",
        },
        {
            "id": 2,
            "name": "Walter",
            "ingested_date": "2022-01-02",
            "op": "I",
        },
    ]
    dataframe = fixture_spark_session.createDataFrame(data)
    return dataframe


@pytest.fixture(scope="session")
def fixture_df_null_field(fixture_spark_session) -> DataFrame:
    data = [
        {
            "id": 1,
            "name": None,
            "ingested_date": "2022-01-01",
            "op": "I",
        },
        {
            "id": 2,
            "name": None,
            "ingested_date": "2022-01-02",
            "op": "I",
        },
    ]
    dataframe = fixture_spark_session.createDataFrame(data)
    return dataframe


@pytest.fixture(scope="session")
def fixture_df_empty(fixture_spark_session) -> DataFrame:
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("ingested_date", TimestampType(), True),
            StructField("op", StringType(), True),
        ]
    )
    data = []
    dataframe = fixture_spark_session.createDataFrame(data, schema)
    return dataframe
