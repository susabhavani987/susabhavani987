
import sys
from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType
import sys
print(sys.executable)

sys.path.append(r"C:\exegithub\lib\site-packages")
from kafka import KafkaConsumer
test = "45660Living"
schema = StructType().add("name", StringType()).add("age", StringType())
uri = f"mongodb+srv://vijaychava101:{test}@school.6nof4yc.mongodb.net/Trust?retryWrites=true&w=majority&authSource=admin"

spark = SparkSession.builder \
        .appName("MyMongoSparkApp") \
        .master("local[2]") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .config("spark.mongodb.write.connection.uri", uri) \
        .getOrCreate()
print(f"this is spark")

df_kafka = spark.readStream.format("kafka") \
          .option("kafka.bootstrap.servers", "d1uqmc63h0primvt047g.any.us-east-1.mpx.prd.cloud.redpanda.com:9092") \
          .option("subscribe", "demo-topic") \
          .option("sasl_mechanism","SCRAM-SHA-256") \
          .option("sasl_plain_username","Ghattamaneni") \
          .option("sasl_plain_password","Livingstone#") \
          .load()
print(f"this is kafka") 
df_kafka.printSchema()

debug_query = df_kafka \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

debug_query.awaitTermination(30) 

df_kafka.selectExpr(
                    "CAST(key AS STRING)", 
                    "CAST(value AS STRING)", 
                    "topic", "partition", "offset"  
                    ).writeStream \
                    .format("console") \
                    .option("truncate", False) \
                    .start() \
                    .awaitTermination(60)
    
json_df = df_kafka.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data")) \
            .select("data.*")
print(f"this is kafka json_df")
query = json_df.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/kafka-mongo-checkpoint") \
        .option("database", "Trust") \
        .option("collection", "students") \
        .trigger(processingTime="10 seconds") \
        .start()

query.awaitTermination(120)