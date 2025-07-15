from pyspark.sql import SparkSession
import threading
import os
from pymongo import MongoClient

def main():
    # Get password from environment variable
    mongo_password = os.getenv("MONGO_TOK")
    print(f"hello s {mongo_password}")
    # MongoDB URI with f-string interpolation
    ##uri = f"mongodb+srv://Vijaychava101:{mongo_password}@school.6nof4yc.mongodb.net/?retryWrites=true&w=majority&appName=school"
    uri = f"mongodb+srv://Vijaychava101:{mongo_password}@school.6nof4yc.mongodb.net/Trust?retryWrites=true&w=majority&authSource=admin"

    ##uri = f"mongodb+srv://vijaychava101:45660Living@school.6nof4yc.mongodb.net/?retryWrites=true&w=majority"
   

    print("Connecting to:", uri)

    # Create Spark session with MongoDB connector
    spark = SparkSession.builder \
        .appName("MyMongoSparkApp") \
        .master("local[2]") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .config("spark.mongodb.read.connection.uri", uri) \
        .getOrCreate()
    print("Connecting after spark", uri)
    # Set up MongoDB client (for direct PyMongo access if needed)
    client = MongoClient(uri)
    db = client['Trust']
    students = db['students']
    dept = db['dept']

    # Multithreaded processing functions
    def process_data1():
        students_df = spark.read.format("mongodb") \
            .option("spark.mongodb.read.connection.uri", uri) \
            .option("database", "Trust") \
            .option("collection", "students") \
            .load()
        filtered_df = students_df.filter(students_df.Dept == 4)
        filtered_df.show()

    def process_data2():
        departments_df = spark.read.format("mongodb") \
            .option("spark.mongodb.read.connection.uri", uri) \
            .option("database", "Trust") \
            .option("collection", "dept") \
            .load()
        filtered_df = departments_df.filter(departments_df.Dept == 4)
        filtered_df.show()

    # Run both processing functions in separate threads
    threads = [threading.Thread(target=fn) for fn in [process_data1, process_data2]]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
