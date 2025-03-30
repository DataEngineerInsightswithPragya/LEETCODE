from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType


spark = SparkSession.builder.appName("customers-who-never-order").getOrCreate()

# Define schema for Customers
table_customers_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Define schema for Orders
table_orders_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("customerId", IntegerType(), True)
])

# Create data for Customers
data_customers = [
    (1, "Joe"),
    (2, "Henry"),
    (3, "Sam"),
    (4, "Max")
]

# Create data for Orders
data_orders = [
    (1, 3),
    (2, 1)
]

# Create DataFrames
customers_df = spark.createDataFrame(data_customers, schema=table_customers_schema)
orders_df = spark.createDataFrame(data_orders, schema=table_orders_schema)

# Show DataFrames
customers_df.show()
orders_df.show()



joined_df = customers_df.join(orders_df,customers_df["id"] == orders_df["customerId"], "left_anti").drop("id")
joined_df.show()