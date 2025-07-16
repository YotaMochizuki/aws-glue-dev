from pyspark.sql import DataFrame
from pyspark.sql.functions import upper


def transform(df: DataFrame) -> DataFrame:
    """ETL処理: 名前を大文字に変換し、追加カラムを付与する."""
    return df.withColumn("name_upper", upper(df["name"]))
