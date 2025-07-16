import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Pytest セッション全体で共有される SparkSession を生成するフィクスチャ.

    - この fixture は `scope="session"` を指定しており、
      テストセッション中に 1 回だけ SparkSession を生成して再利用します.
    - Spark はローカルモード (local[*]) で全コアを使って起動されます.
    - appName は Spark UI やログで確認できるアプリ名です.
    """
    return SparkSession.builder.master("local[*]").appName("TestSession").getOrCreate()


@pytest.fixture
def sample_data(spark):
    """テスト用のサンプル DataFrame を提供するフィクスチャ.

    - `spark` fixture を使って単一レコードの DataFrame を生成します.
    - カラムは `id` と `name` を持ち、値は `(1, "Alice")` です.
    """
    return spark.createDataFrame([(1, "Alice")], ["id", "name"])


def pytest_sessionstart(session):
    """Pytest のテスト実行前に自動的に呼び出される特殊なフック関数.

    - テストの開始時にコンソールにメッセージを出力します.
    - ログ確認や CI/CD における可視性を高めるために便利です.
    """
    print("🚀 テスト開始！")
