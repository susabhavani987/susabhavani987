from pyspark.sql import SparkSession
import threading
import os
from pymongo import MongoClient

def main():
    # Get password from environment variable
    mongo_password = os.getenv("MONGO_PASSWORD")
    test = "45660Living"
    print(f"hello s {mongo_password}")
    # MongoDB URI with f-string interpolation
    ##uri = f"mongodb+srv://Vijaychava101:{mongo_password}@school.6nof4yc.mongodb.net/?retryWrites=true&w=majority&appName=school"
    uri = f"mongodb+srv://vijaychava101:{test}@school.6nof4yc.mongodb.net/Trust?retryWrites=true&w=majority&authSource=admin"

    ##uri = f"mongodb+srv://vijaychava101:45660Living@school.6nof4yc.mongodb.net/?retryWrites=true&w=majority"
   

    print("Connecting to:", uri)

    # Create Spark session with MongoDB connector
    spark = SparkSession.builder \
       .appName("MyMongoSparkApp") \
       .master("local[*]") \
       .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
       .config("spark.mongodb.read.connection.uri", uri) \
       .config("spark.jars.repositories", "https://repos.spark-packages.org") \
       .getOrCreate()
    print("Spark session started.")
    students_df = spark.read.format("mongodb") \
        .option("spark.mongodb.read.database", "Trust") \
        .option("spark.mongodb.read.collection", "students") \
        .load()
    print("Loaded DataFrame")
    print("Record count:", df.count())
    df.printSchema()
    
    for row in df.collect():
       print(row)
    students_df.show()
    
    
   if __name__ == "__main__":
      main()
