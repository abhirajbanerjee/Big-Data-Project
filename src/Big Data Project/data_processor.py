import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as spark_sum, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

kafka_bs = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

spark = SparkSession.builder.appName("UpstoxProcessor").getOrCreate()

# adjust schema to match the streamer message structure
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("ltp", FloatType()),
    StructField("volume", IntegerType()),
])

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bs)
    .option("subscribe", "upstox_ticks")
    .option("startingOffsets", "latest")
    .load()
)

json_df = df.selectExpr("CAST(value AS STRING) as json_str").select(from_json(col("json_str"), schema).alias("data"))
flat = json_df.select("data.*")

# example 1-minute aggregation; adjust window and aggregations as required
agg = (
    flat.groupBy(window(col("timestamp"), "1 minute"))
    .agg(
        spark_sum("volume").alias("total_volume"),
        spark_sum("ltp").alias("sum_ltp"),
    )
)

# prepare JSON to write back to Kafka topic upstox_orderflow
output = agg.select(to_json(struct(
    col("window.start").alias("window_start"),
    col("total_volume"),
    col("sum_ltp")
)).alias("value"))

query = (
    output.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bs)
    .option("topic", "upstox_orderflow")
    .option("checkpointLocation", "/tmp/s
