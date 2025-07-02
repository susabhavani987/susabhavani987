from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("SamplePySparkJob") \
        .getOrCreate()

    # Example: Create DataFrame and write to CSV
    df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
    record_count = df.count()
    ##data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    ##df = spark.createDataFrame(data, ["name", "age"])
    print(f"Number of records: {record_count}")
    filtered_df = df.filter(df.Price > 900 & df.Availability == "discontinued")
    filtered_df.show()
    df.write.mode("overwrite").csv("output/sample_output")

    spark.stop()

if __name__ == "__main__":
    main()
