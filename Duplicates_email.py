from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("employees-earning-more-than-their-managers").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), False),   # id cannot be NULL
    StructField("email", StringType(), False) # email cannot be NULL
])

# Define data
data = [
    (1, "a@b.com"),
    (2, "c@d.com"),
    (3, "a@b.com")
]

email_df = spark.createDataFrame(data,schema)
email_df.show()

df = email_df.groupBy(email_df["email"]).agg(count(email_df["email"]).alias("cnt_email"))
df.show()

result_df = df.where(df["cnt_email"] > 1 ).drop("cnt_email")
result_df.show()