from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.appName("combine-two-tables").getOrCreate()

# Define schema for Person table
person_schema = StructType([
    StructField("personId", IntegerType(), nullable=False),
    StructField("lastName", StringType(), nullable=False),
    StructField("firstName", StringType(), nullable=False)
])

# Define schema for Address table
address_schema = StructType([
    StructField("addressId", IntegerType(), nullable=False),
    StructField("personId", IntegerType(), nullable=False),
    StructField("city", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False)
])

# Create Person DataFrame
person_data = [
    (1, "Wang", "Allen"),
    (2, "Alice", "Bob")
]
person_df = spark.createDataFrame(person_data, schema=person_schema)

# Create Address DataFrame
address_data = [
    (1, 2, "New York City", "New York"),
    (2, 3, "Leetcode", "California")
]
address_df = spark.createDataFrame(address_data, schema=address_schema)

joined_df = person_df.join(address_df,person_df["personId"] == address_df["personId"],"left").drop(address_df["personId"])
result_df = joined_df.select("firstName","lastName","city","state")
result_df.show()