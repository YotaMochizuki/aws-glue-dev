import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """テストで使用する共通の SparkSession を返します."""
    return SparkSession.builder.appName("Test").getOrCreate()


def test_dataframe(spark):
    """DataFrame を作成して、行数が 1 件であることを検証します."""
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    assert df.count() == 1
