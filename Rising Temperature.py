from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType,DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,rank,to_date,lag,lead,date_diff


spark = SparkSession.builder.appName("Rising Temperatures").getOrCreate()


# First create with StringType for dates
schema = StructType([
    StructField("id", IntegerType()),
    StructField("recordDate", StringType()),
    StructField("temperature", IntegerType())
])

data = [
    (1, "2015-01-01", 10),
    (2, "2015-01-02", 25),
    (3, "2015-01-03", 20),
    (4, "2015-01-04", 30)
]

df = spark.createDataFrame(data, schema)

# Then convert to DateType
df = df.withColumn("recordDate", to_date("recordDate", "yyyy-MM-dd"))

df.show()
df.printSchema()

WindowSpec = Window.orderBy(df["recordDate"].asc())

lag_df = df.withColumn("lag_temp", lag("temperature").over(WindowSpec))\
            .withColumn("lag_recordDate", lag("recordDate").over(WindowSpec))\
            .withColumn("date_differences", date_diff("recordDate","lag_recordDate"))
lag_df.show()


result_df = lag_df.where((lag_df["temperature"] > lag_df["lag_temp"]) & (lag_df["date_differences"] == 1))\
                  .select("id")

result_df.show()