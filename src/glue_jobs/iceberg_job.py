import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
import boto3
from botocore.exceptions import ClientError


def get_logger(
    level: int = logging.INFO,
    format: str = "%(asctime)s [%(filename)s] [%(funcName)s] [%(levelname)s] %(message)s",
    logging_file_handler: str = "/tmp/filename.log",
):
    handlers = [logging.FileHandler(logging_file_handler), logging.StreamHandler()]
    logging.basicConfig(level=level, format=format, handlers=handlers)
    logger = logging.getLogger()
    logger.setLevel(level)
    return logger


logger = get_logger()


class RawToStageIcebergJob:
    def __init__(
        self,
        spark: SparkSession,
        glue_context: GlueContext,
        iceberg_db: str,
        iceberg_table: str,
        source_db: str,
        source_table: str,
        partition_by: str = "NONE",
        iceberg_table_update_mode: str = "append",
        catalog: str = "glue_catalog",  # replace this to 'local' for local tests
        region_name: str = "us-east-1",
    ):
        self.spark = spark
        self.glue_context = glue_context
        self.iceberg_db = iceberg_db
        self.iceberg_table = iceberg_table
        self.partition_by = partition_by
        self.source_db = source_db
        self.source_table = source_table
        self.iceberg_table_update_mode = iceberg_table_update_mode
        self.region_name = region_name
        self._iceberg_table_update_mode = self.get_iceberg_table_update_mode
        self._tmp_source_spark_table = "tmp_source_table"
        self._partition_by_statement = self.get_partition_by_statement
        self._iceberg_table_full_path = f"{catalog}.{self.iceberg_db}.{self.iceberg_table}"

    @property
    def get_partition_by_statement(self):
        return f"PARTITIONED BY ({self.partition_by})" if self.partition_by != "NONE" else ""

    @property
    def get_iceberg_table_update_mode(self):
        if self.iceberg_table_update_mode == "append":
            return self.iceberg_table_update_mode
        elif self.iceberg_table_update_mode == "overwrite":
            raise NotImplementedError("Overwrite Mode is not implemented yet")
        elif self.iceberg_table_update_mode == "upsert":
            raise NotImplementedError("Upsert Mode is not implemented yet")
        else:
            raise ValueError(f"Invalid Iceberg Table Update Mode: {self.iceberg_table_update_mode}")

    def get_iceberg_table_create_query(self):
        query = f"""
        CREATE TABLE {self._iceberg_table_full_path}
        USING iceberg
        {self._partition_by_statement}
        TBLPROPERTIES ("format-version"="2")
        AS SELECT * FROM {self._tmp_source_spark_table}
        """
        return query

    def read_source_table(self) -> DataFrame:
        dyf = self.glue_context.create_dynamic_frame.from_catalog(
            database=self.source_db,
            table_name=self.source_table,
            transformation_ctx=self.source_table,
        )
        df = dyf.toDF()
        return df

    def check_if_iceberg_table_exists(self) -> bool:
        glue_client = boto3.client("glue", region_name=self.region_name)

        try:
            glue_client.get_table(DatabaseName=self.iceberg_db, Name=self.iceberg_table)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                return False
            else:
                raise

    def create_iceberg_table(self, df: DataFrame) -> None:
        df.createOrReplaceTempView(self._tmp_source_spark_table)

        create_iceberg_table_query = self.get_iceberg_table_create_query()
        self.spark.sql(create_iceberg_table_query)
        logger.info(f"Iceberg Table Created: {self._iceberg_table_full_path}")
        return

    def append_to_iceberg_table(self, df: DataFrame) -> None:
        df.createOrReplaceTempView(self._tmp_source_spark_table)

        append_to_iceberg_table_query = f"""
        INSERT INTO {self._iceberg_table_full_path}
        SELECT *
        FROM {self._tmp_source_spark_table}
        """
        self.spark.sql(append_to_iceberg_table_query)
        logger.info(f"Appended to Iceberg Table: {self._iceberg_table_full_path}")
        return

    def overwrite_iceberg_table(self, df: DataFrame):
        raise NotImplementedError("Overwrite Mode is not implemented yet")

    def upsert_iceberg_table(self, df: DataFrame):
        raise NotImplementedError("Upsert Mode is not implemented yet")

    def update_iceberg_table(self, df: DataFrame) -> None:
        if self._iceberg_table_update_mode == "append":
            self.append_to_iceberg_table(df)
        elif self._iceberg_table_update_mode == "overwrite":
            self.overwrite_iceberg_table(df)
        elif self._iceberg_table_update_mode == "upsert":
            self.upsert_iceberg_table(df)
        else:
            raise ValueError(f"Invalid Iceberg Table Update Mode: {self.iceberg_table_update_mode}")

    def create_or_update_iceberg_table(self) -> None:
        source_df = self.read_source_table()
        row_count = source_df.count()

        if row_count > 0:
            iceberg_table_exist = self.check_if_iceberg_table_exists()
            if not iceberg_table_exist:
                self.create_iceberg_table(source_df)
            else:
                self.update_iceberg_table(source_df)
        else:
            logger.info("No new records to ingest into Iceberg Table.")
            return


if __name__ == "__main__":
    job_args = [
        "job_name",
        "source_db",
        "source_table",
        "iceberg_db",
        "iceberg_table",
        "iceberg_s3_bucket",
        "partition_by",
        "iceberg_table_update_mode",
    ]
    args = getResolvedOptions(sys.argv, job_args)

    # Spark Session
    warehouse = f"s3://{args['iceberg_s3_bucket']}/iceberg/"
    spark = (
        SparkSession.builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", warehouse)
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )
    # Add KMS Information to Write to S3 KMS Encrypted Buckets
    spark._jsc.hadoopConfiguration().set("fs.s3.enableServerSideEncryption", "true")

    # Glue Context
    glueContext = GlueContext(spark)
    job = Job(glueContext)
    job.init(args["job_name"], args)

    # iceberg job initialization / run
    iceberg_upsert_job = RawToStageIcebergJob(
        spark=spark,
        glue_context=glueContext,
        iceberg_db=args["iceberg_db"],
        iceberg_table=args["iceberg_table"],
        source_db=args["source_db"],
        source_table=args["source_table"],
        partition_by=args["partition_by"],
        iceberg_table_update_mode=args["iceberg_table_update_mode"],
    )
    iceberg_upsert_job.create_or_update_iceberg_table()
    job.commit()
