from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("KafkaToBronze")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sales")
    .option("startingOffsets", "latest")
    .load()
)

json_df = df.selectExpr("CAST(value AS STRING) as json")

query = (
    json_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "bronze/checkpoint"
    )
    .start("bronze/orders")
)

query.awaitTermination()