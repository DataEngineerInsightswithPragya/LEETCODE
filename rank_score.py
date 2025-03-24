from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import Window
from pyspark.sql.functions import col, dense_rank

# Create Spark session
spark = SparkSession.builder.appName("ScoreRanking").getOrCreate()

# Define schema using StructType
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("score", DoubleType(), nullable=False)
])

# Sample data
data = [
    (1, 3.50),
    (2, 3.65),
    (3, 4.00),
    (4, 3.85),
    (5, 4.00),
    (6, 3.65)
]

# Create DataFrame with explicit schema
scores_df = spark.createDataFrame(data, schema=schema)

windowspec = Window.orderBy(scores_df['score'].desc())

df = scores_df.withColumn("rank_number",dense_rank().over(windowspec)).select("score","rank_number")
df.show()