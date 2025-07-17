from pyspark.sql import SparkSession
import threading
import os
from pymongo import MongoClient

def main():
    # Get password from environment variable
    mongo_password = os.getenv("MONGO_PASSWORD")
    ##test = "45660Living"
    print(f"hello s {mongo_password}")
    # MongoDB URI with f-string interpolation
    ##uri = f"mongodb+srv://Vijaychava101:{mongo_password}@school.6nof4yc.mongodb.net/?retryWrites=true&w=majority&appName=school"
    uri = f"mongodb+srv://vijaychava101:{mongo_password}@school.6nof4yc.mongodb.net/Trust?retryWrites=true&w=majority&authSource=admin"

    ##uri = f"mongodb+srv://vijaychava101:45660Living@school.6nof4yc.mongodb.net/?retryWrites=true&w=majority"
    
    

    print("Connecting to:", uri)

    # Create Spark session with MongoDB connector
    spark = SparkSession.builder \
        .appName("MyMongoSparkApp") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .config("spark.mongodb.write.connection.uri", uri) \
        .getOrCreate()
    
    df_csv = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
    record_count = df_csv.count()
    print(f"Number of records: {record_count}")
   
    
    df_csv.write \
    .format("mongodb") \
    .option("database", "Trust") \
    .option("collection", "csv_data") \
    .mode("overwrite") \
    .save()
if __name__ == "__main__":
    main()
