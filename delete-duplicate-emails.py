from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,rank


spark = SparkSession.builder.appName("delete-duplicates-emails").getOrCreate()

# Define schema for Person
table_person_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True)
])

# Create data for Person
data_person = [
    (1, "john@example.com"),
    (2, "bob@example.com"),
    (3, "john@example.com")
]

# Create DataFrame
person_df = spark.createDataFrame(data_person, schema=table_person_schema)

# Show DataFrame
person_df.show()


WindowSpec = Window.partitionBy(person_df["email"]).orderBy(person_df["id"].asc())

rank_df = person_df.withColumn("rank_num",rank().over(WindowSpec))
rank_df.show()

filter_df = rank_df.where(rank_df["rank_num"] == 1).drop(rank_df["rank_num"]).orderBy(rank_df["id"].asc())
filter_df.show()