from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,max
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

spark = SparkSession.builder.appName('second-highest-salary').getOrCreate()

# Define schema using StructType
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

# Create data
data = [
    (1, 100),
    (2, 200),
    (3, 300)
]

# Create DataFrame
employee_df = spark.createDataFrame(data, schema=schema)

windowspec = Window.orderBy(employee_df['salary'].desc())

dense_rank_df = employee_df.withColumn("rank_number",
                                       dense_rank().over(windowspec))


dense_rank_df.show()

result_df = dense_rank_df.where(dense_rank_df["rank_number"] == 2).select(dense_rank_df["salary"].alias("SecondHighestSalary"))
result_df.show()