import os

from awsglue.context import GlueContext
from pyspark.sql import Row, SparkSession

# Iceberg + S3Tables (HadoopCatalog) 用 SparkSession を構築
spark = (
    SparkSession.builder.appName("Glue5-Iceberg-S3Tables")
    .config("spark.jars", "/jars/s3-tables-catalog-for-iceberg-runtime-0.1.4.jar")
    .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.s3tablesbucket.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
    .config("spark.sql.catalog.s3tablesbucket.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(
        "spark.sql.catalog.s3tablesbucket.warehouse",
        os.getenv("S3_TABLES_BUCKET", ""),
    )
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

# Spark 設定の確認
print("🔧 現在の Spark 設定:")
for k, v in spark.sparkContext.getConf().getAll():
    if "catalog" in k or "iceberg" in k:
        print(f"{k} = {v}")

# GlueContext を取得（必要なら）
glue_context = GlueContext(spark.sparkContext)

# デモデータ作成
data = [Row(id=1, name="Alice", age=30), Row(id=2, name="Bob", age=25), Row(id=3, name="Charlie", age=35)]
df = spark.createDataFrame(data)

print("🔍 元のデータフレーム:")
df.show()

# 完全修飾名の Iceberg テーブル
table_name = "s3tablesbucket.default.sample_table"

# ネームスペースの作成（存在しない場合）
print("🗂 ネームスペース `s3tablesbucket.default` の作成確認中...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS `s3tablesbucket`.`default`")

# Iceberg テーブルの作成
print(f"📦 Iceberg テーブル `{table_name}` を作成中...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT,
        name STRING,
        age INT
    )
    USING iceberg
""")

# Iceberg テーブルへ書き込み
print(f"📤 Iceberg テーブル `{table_name}` に書き込み中...")
df.writeTo(table_name).append()

# Iceberg テーブルから読み取り
print(f"📥 Iceberg テーブル `{table_name}` から読み取り中...")
df_loaded = spark.read.table(table_name)

print("📄 Iceberg テーブルの内容:")
df_loaded.show()
