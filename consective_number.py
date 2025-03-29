from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import lag,lead

spark = SparkSession.builder.appName("consecutive number").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("num", IntegerType(), True)
])

# Define data
data = [
    (1, 1),
    (2, 1),
    (3, 1),
    (4, 2),
    (5, 1),
    (6, 2),
    (7, 2)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

df.show()

WindowSpec = Window.orderBy(df["id"].asc())

lead_lag_df = df.withColumn("lag_num",lag(df["num"]).over(WindowSpec))\
                .withColumn("lead_num",lead(df["num"]).over(WindowSpec))

lead_lag_df.show()

final_df = lead_lag_df.where((lead_lag_df["num"] == lead_lag_df["lag_num"]) & (lead_lag_df["num"] == lead_lag_df["lead_num"]) & (lead_lag_df["lead_num"] == lead_lag_df["lag_num"]))\
                      .select(lead_lag_df["num"].alias("ConsecutiveNums")).distinct()


final_df.show()

