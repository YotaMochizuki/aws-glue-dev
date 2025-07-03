from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import Row

# Sparkコンテキストの初期化
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# デモデータの作成
data = [Row(id=1, name="Alice", age=30), Row(id=2, name="Bob", age=25), Row(id=3, name="Charlie", age=35)]

df = spark.createDataFrame(data)

# DataFrame の表示
print("🔍 データフレームの内容:")
df.show()

# Glue DynamicFrame に変換して表示
dyf = DynamicFrame.fromDF(df, glue_context, "sample_dyf")
print("🔍 DynamicFrame の内容:")
dyf.show()
