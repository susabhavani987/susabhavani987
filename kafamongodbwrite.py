
import sys
from pyspark.sql import SparkSession
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaDebug") \
    .master("local[2]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

spark.sparkContext.setLogLevel("DEBUG")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "d1uqmc63h0primvt047g.any.us-east-1.mpx.prd.cloud.redpanda.com:9092") \
    .option("subscribe", "demo-topic") \
    .option("security.protocol", "SASL_SSL") \
    .option("sasl.mechanism", "SCRAM-SHA-256") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required username='Ghattamaneni' password='Livingstone#';") \
    .option("kafka.request.timeout.ms", "60000") \
    .option("kafka.metadata.max.age.ms", "30000") \
    .load()

df.selectExpr("CAST(value AS STRING)").writeStream \
    .format("console") \
    .option("checkpointLocation", "file:///tmp/kafka_debug_checkpoint") \
    .start() \
    .awaitTermination()


