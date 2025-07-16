import os

from modules.etl import transform
from modules.session import create_spark_session


def log_environment_variables():
    """ETL処理で使用する環境変数をログ出力する."""
    print("📌 使用される環境変数:")
    print(f"  AWS_REGION = {os.getenv('AWS_REGION', 'ap-northeast-1')}")
    print(f"  S3_TABLES_BUCKET = {os.getenv('S3_TABLES_BUCKET', '(未設定)')}")
    print(f"  S3_TABLES_CATALOG_NAME = {os.getenv('S3_TABLES_CATALOG_NAME', '(未設定)')}")
    print(f"  S3_TABLES_NAME_SPACE = {os.getenv('S3_TABLES_NAME_SPACE', '(未設定)')}")
    print(f"  S3_TTABLES_TABLE_NAME = {os.getenv('S3_TTABLES_TABLE_NAME', '(未設定)')}")
    print(f"  JOB_NAME = {os.getenv('JOB_NAME', '(未設定)')}")
    print("-" * 60)


def run_etl():
    """ETL ジョブを実行するメイン関数.

    Spark セッションを作成し、デモデータを変換し、Iceberg テーブルに書き込んだあと結果を表示する.
    """
    spark = create_spark_session()

    # デモデータ
    data = [(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]
    df = spark.createDataFrame(data, ["id", "name", "age"])
    transformed_df = transform(df)

    # テーブル情報
    catalog = os.getenv("S3_TABLES_CATALOG_NAME", "s3tablesbucket")
    namespace = os.getenv("S3_TABLES_NAME_SPACE", "default")
    table = os.getenv("S3_TTABLES_TABLE_NAME", "sample_etl_table")
    table_name = f"{catalog}.{namespace}.{table}"

    # Iceberg ネームスペース作成
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")

    # テーブル作成
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

    # データ書き込み
    print(f"📤 Iceberg テーブル `{table_name}` に書き込み中...")
    transformed_df.writeTo(table_name).append()

    # 結果表示
    print("📄 Iceberg テーブルから読み取り:")
    spark.read.table(table_name).show()


if __name__ == "__main__":
    log_environment_variables()
    run_etl()
