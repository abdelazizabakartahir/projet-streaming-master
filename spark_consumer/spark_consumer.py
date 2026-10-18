import os
import sys
import time

# Configuration Hadoop Windows
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, LongType

spark = SparkSession.builder \
    .appName("SparkStreamingVentes") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("transaction_id", IntegerType()) \
    .add("produit", StringType()) \
    .add("prix", DoubleType()) \
    .add("quantite", IntegerType()) \
    .add("timestamp", LongType())

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ventes-stream") \
    .load()

df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Conversion du timestamp et calcul de la latence (Exigence du barème : Mesure)
df_formatted = df_parsed.withColumn(
    "event_time", (col("timestamp") / 1000.0).cast("timestamp")
).withColumn(
    "latence_ms", (unix_timestamp(current_timestamp()) - col("timestamp") / 1000) * 1000
)

# Agrégation par fenêtre glissante
df_windowed = df_formatted \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("produit")
    ) \
    .agg({"prix": "sum", "latence_ms": "avg"}) \
    .withColumnRenamed("sum(prix)", "ca_total") \
    .withColumnRenamed("avg(latence_ms)", "latence_moyenne_ms")

query = df_windowed.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()