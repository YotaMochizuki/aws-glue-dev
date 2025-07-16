import os

from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper


def create_spark_session() -> SparkSession:
    """SparkSession を構築する."""
    return (
        SparkSession.builder.appName("Glue5-Iceberg-S3Tables")
        .config("spark.jars", "/jars/s3-tables-catalog-for-iceberg-runtime-0.1.4.jar")
        .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
        .config("spark.sql.catalog.s3tablesbucket.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.s3tablesbucket.warehouse", os.getenv("S3_TABLES_BUCKET", ""))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.hadoop.aws.region", os.getenv("AWS_REGION", "ap-northeast-1"))
        .getOrCreate()
    )


def transform(df):
    """ETL処理: 名前を大文字に変換し、追加カラムを付与する."""
    return df.withColumn("name_upper", upper(df["name"]))


def run_etl():
    """ETL処理のメイン関数."""
    spark = create_spark_session()
    _ = GlueContext(spark.sparkContext)

    # デモデータ
    data = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]
    df = spark.createDataFrame(data, ["id", "name", "age"])
    transformed_df = transform(df)

    # テーブル情報
    catalog = "s3tablesbucket"
    namespace = "default"
    table = "sample_etl_table"
    table_name = f"{catalog}.{namespace}.{table}"

    # ネームスペース作成
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")

    # Iceberg テーブル作成
    print(f"📦 Iceberg テーブル `{table_name}` を作成中...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT,
            name STRING,
            age INT,
            name_upper STRING
        )
        USING iceberg
    """)

    # Iceberg テーブル書き込み
    print(f"📤 Iceberg テーブル `{table_name}` に書き込み中...")
    transformed_df.writeTo(table_name).append()

    # 結果表示
    print("📄 Iceberg テーブルから読み取り:")
    result = spark.read.table(table_name)
    result.show()


if __name__ == "__main__":
    run_etl()
