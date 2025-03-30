from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank


spark = SparkSession.builder.appName("department-highest-salary").getOrCreate()

# Define schema for Employee
table_employee_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("departmentId", IntegerType(), True)
])

# Define schema for Department
table_department_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Create data for Employee
data_employee = [
    (1, "Joe", 70000, 1),
    (2, "Jim", 90000, 1),
    (3, "Henry", 80000, 2),
    (4, "Sam", 60000, 2),
    (5, "Max", 90000, 1)
]

# Create data for Department
data_department = [
    (1, "IT"),
    (2, "Sales")
]

# Create DataFrames
employee_df = spark.createDataFrame(data_employee, schema=table_employee_schema)
department_df = spark.createDataFrame(data_department, schema=table_department_schema)

# Show DataFrames
employee_df.show()
department_df.show()


WindowSpec = Window.partitionBy(employee_df["departmentId"]).orderBy(employee_df["salary"].desc())

joined_df = employee_df.join(department_df,employee_df["departmentId"] == department_df["id"],"inner").drop(department_df["id"]).withColumn("Department",department_df["name"]).drop(department_df["name"])
joined_df.show()

dense_df = joined_df.withColumn("rank_num",dense_rank().over(WindowSpec))
dense_df.show()

result_df = dense_df.where(dense_df["rank_num"] == 1).select("Department",dense_df["name"].alias("Employee"),dense_df["salary"].alias("Salary"))
result_df.show()




