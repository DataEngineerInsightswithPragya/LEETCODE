from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import lag,lead,col

spark = SparkSession.builder.appName("employees-earning-more-than-their-managers").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("salary", FloatType(), False),
    StructField("managerId", IntegerType(), True)  # Allows NULL values
])

# Define data
data = [
    (1, "Joe", 70000.0, 3),
    (2, "Henry", 80000.0, 4),
    (3, "Sam", 60000.0, None),  # None represents NULL in PySpark
    (4, "Max", 90000.0, None)
]

# Create DataFrame
employee_df = spark.createDataFrame(data, schema=schema)
employee_df.show()

e = employee_df.alias("e")
m = employee_df.alias("m")


joined_df = e.join(m, col("e.managerId") == col("m.id"), "inner")\
            .where(col("e.salary") > col("m.salary"))\
            .select(col("e.name").alias("Employee"))
joined_df.show()
