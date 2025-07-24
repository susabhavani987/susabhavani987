from pyspark.sql import SparkSession
import threading
import os


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PartitionExample") \
    .getOrCreate()

# Read CSV file
df = spark.read.csv("/data/sample_students_100k.csv", header=True, inferSchema=True)

# Show the number of initial partitions
print("Initial partitions:", df.rdd.getNumPartitions())

# Repartition the DataFrame by a column (for better parallelism)
df_repartitioned = df.repartition("country")

print("Repartitioned by 'country':", df_repartitioned.rdd.getNumPartitions())

# Do some transformations (e.g., filter or aggregate)
filtered_df = df_repartitioned.filter(df["year"] >= 2023)

# Write the result as partitioned Parquet files by country and year
filtered_df.write \
    .mode("overwrite") \
    .partitionBy("country", "year") \
    .parquet("/output/students_partitioned/")

