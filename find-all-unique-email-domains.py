from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import count,substring_index

spark = SparkSession.builder.appName("find-all-unique-email-domains").getOrCreate()

data = [
    (336, "hwkiy@test.edu"),
    (489, "adcmaf@outlook.com"),
    (449, "vrzmwyum@yahoo.com"),
    (95,  "tof@test.edu"),
    (320, "jxhbagkpm@example.org"),
    (411, "zxcf@outlook.com")
]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True)
])

df = spark.createDataFrame(data,schema)
df.show()


df1 = df.where(df['email'].like('%.com'))
df1.show()

df2 = df1.withColumn("email_domain",substring_index(df1['email'],'@',-1))
df2.show()

df3 = df2.groupBy(df2['email_domain']).agg(count(df2["email_domain"]).alias("count"))
df3.show()





