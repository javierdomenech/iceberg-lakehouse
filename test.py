from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize SparkSession
spark = SparkSession.builder.appName("SetNullableFalse").getOrCreate()

# Data for DataFrames
data = [("Alice", 1), ("Bob", 2), ("Catherine", 3)]

# Schema with nullable = False for the 'id' column
schema_non_nullable = StructType([
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), False)  # Setting nullable = False
])

# Schema with nullable = True for the 'id' column
schema_nullable = StructType([
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True)  # Setting nullable = True
])

# Create DataFrames with the specified schemas
df_non_nullable = spark.createDataFrame(data, schema=schema_non_nullable)
df_nullable = spark.createDataFrame(data, schema=schema_nullable)

# Show schemas and data
print("Schema with 'id' as nullable = False:")
df_non_nullable.printSchema()
df_non_nullable.show()

print("Schema with 'id' as nullable = True:")
df_nullable.printSchema()
df_nullable.show()