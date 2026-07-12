from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("BronzeToSilver")
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

# Bronze schema
schema = StructType([

    StructField("OrderID", IntegerType()),

    StructField("Customer", StringType()),

    StructField("City", StringType()),

    StructField("Product", StringType()),

    StructField("Quantity", IntegerType()),

    StructField("Price", IntegerType()),

    StructField("Timestamp", StringType())

])

# Read Bronze
bronze = (
    spark.readStream
    .format("delta")
    .load("bronze/orders")
)

# Parse JSON
silver = bronze.select(

    from_json(col("json"), schema).alias("data")

).select("data.*")

# Data Cleaning

# Remove duplicates

silver = silver.dropDuplicates()

# Remove nulls

silver = silver.na.drop()

# Convert timestamp

silver = silver.withColumn(
    "Timestamp",
    to_timestamp("Timestamp")
)

# Write Silver
query = (

    silver.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "silver/checkpoint"
    )
    .start("silver/orders")

)

query.awaitTermination()