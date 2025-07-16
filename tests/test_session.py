from modules.session import create_spark_session


def test_create_spark_session():
    """create_spark_session() が None ではなく、SparkSession を正しく生成するかを確認します.

    appName が空ではなく設定されているかを確認します.
    """
    spark = create_spark_session()
    assert spark is not None
    assert spark.sparkContext.appName.startswith("GlueJob") or spark.sparkContext.appName != ""
