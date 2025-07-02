from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("SamplePySparkJob") \
        .getOrCreate()

    # Example: Create DataFrame and write to CSV
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    df = spark.createDataFrame(data, ["name", "age"])

    df.show()
    df.write.mode("overwrite").csv("output/sample_output")

    spark.stop()

if __name__ == "__main__":
    main()
