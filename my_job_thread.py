from pyspark.sql import SparkSession
import threading

def main():
    spark = SparkSession.builder \
    .appName("Multithreaded PySpark Example") \
    .master("local[3]") \
    .getOrCreate()

    # Example: Create DataFrame and write to CSV
    df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
    record_count = df.count()
    print(f"Number of records: {record_count}")

    df.write.mode("overwrite").csv("output/sample_output")

    
    def process_data1():
       local_df = df
       filtered_df = local_df.filter((local_df.Price > 900) & (local_df.Availability == "discontinued"))
       filtered_df.show()

    def process_data2():
       filtered_df = local_df.filter((local_df.Price < 900) & (local_df.Availability == "discontinued"))
       filtered_df.show()

    def process_data3():
       filtered_df = local_df.filter((local_df.Price == 900) & (local_df.Availability == "discontinued"))
       filtered_df.show()


    threads = [threading.Thread(target=fn) for fn in [process_data1, process_data2, process_data3]]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


    spark.stop()
if __name__ == "__main__":
    main()
