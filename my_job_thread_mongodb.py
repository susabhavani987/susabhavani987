from pyspark.sql import SparkSession
import threading
from pymongo import MongoClient
def main():

   mongo_password = os.getenv("MONGO_PASSWORD")
   spark = SparkSession.builder \
      .appName("MongoDBJoinExample") \
      ##.config("spark.mongodb.read.connection.uri","uri="mongodb+srv://vijaychava101:QAMyb1exS4BNHooQ@Trust.6nof4yc.mongodb.net/?retryWrites=true&w=majority&appName=Trust") \
      .config("spark.mongodb.read.connection.uri", "mongodb+srv://Vijaychava101:{mongo_password}@Trust.6nof4yc.mongodb.net/?retryWrites=true&w=majority&appName=Trust") \
      .getOrCreate()

# Read "students" collection
    students_df = spark.read.format("mongodb").option("database", "Trust").option("collection", "students").load()

# Read "departments" collection
    departments_df = spark.read.format("mongodb").option("database", "Trust").option("collection", "departments").load()

    spark = SparkSession.builder \
    .appName("Multithreaded PySpark Example") \
    .master("local[2]") \
    .getOrCreate()

    # Example: Create DataFrame and write to CSV
    ##df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
    ##record_count = df.count()
    ##print(f"Number of records: {record_count}")

    ##df.write.mode("overwrite").csv("output/sample_output")
    uri = "mongodb+srv://vijaychava101:QAMyb1exS4BNHooQ@Trust.6nof4yc.mongodb.net/?retryWrites=true&w=majority&appName=Trust"
    client = MongoClient(uri)

      # Access your database and collection
    db = client['Trust']
    students = db['students']
    dept = db['dept']
    
    def process_data1():
       students_df = spark.read.format("mongodb").option("database", "Trust").option("collection", "students").load()
       filtered_df = students_df.filter((students_df.Dept == 4) )
       filtered_df.show()

    def process_data2():
       departments_df = spark.read.format("mongodb").option("database", "Trust").option("collection", "departments").load()
       filtered_df = departments_df.filter((departments_df.Dept == 4))
       filtered_df.show()

   


    threads = [threading.Thread(target=fn) for fn in [process_data1, process_data2]]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


    spark.stop()
if __name__ == "__main__":
    main()
