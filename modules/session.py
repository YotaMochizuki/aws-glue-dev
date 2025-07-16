import os

from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """SparkSession を構築する."""
    catalog = os.getenv("S3_TABLES_CATALOG_NAME", "s3tablesbucket")
    return (
        SparkSession.builder.appName(os.getenv("JOB_NAME", "GlueJob"))
        .config("spark.jars", "/jars/s3-tables-catalog-for-iceberg-runtime-0.1.4.jar")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.warehouse", os.getenv("S3_TABLES_BUCKET", ""))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.hadoop.aws.region", os.getenv("AWS_REGION", "ap-northeast-1"))
        .getOrCreate()
    )
