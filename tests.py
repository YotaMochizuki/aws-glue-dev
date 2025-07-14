# test_etl.py
import pytest
from pyspark.sql import SparkSession

from main import transform  # ファイル名が etl_script.py の場合は適宜変更


@pytest.fixture(scope="session")
def spark():
    """テスト用の SparkSession を作成."""
    return SparkSession.builder.master("local[*]").appName("ETL Test").getOrCreate()


def test_transform_adds_uppercase_column(spark):
    """Transform 関数が name_upper カラムを追加し、正しく大文字変換されるか確認."""
    input_data = [(1, "alice", 30), (2, "bob", 25)]
    df = spark.createDataFrame(input_data, ["id", "name", "age"])

    result_df = transform(df)
    result = result_df.select("id", "name_upper").collect()

    assert result[0]["name_upper"] == "ALICE"
    assert result[1]["name_upper"] == "BOB"
