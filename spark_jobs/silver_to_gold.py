from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("SilverToGold")
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

# Read Silver
silver = (
    spark.readStream
    .format("delta")
    .load("silver/orders")
)

#Business Aggregation

#We will calculate

#Total Sales
#Total Orders
#Total Quantity per City.

gold = (

    silver
    .groupBy("City")

    .agg(

        count("OrderID").alias("TotalOrders"),

        sum("Quantity").alias("TotalQuantity"),

        sum(expr("Quantity * Price")).alias("TotalSales")

    )

)

# Write Gold
query = (

    gold.writeStream

    .format("delta")

    .outputMode("complete")

    .option(
        "checkpointLocation",
        "gold/checkpoint"
    )

    .start("gold/orders")

)

query.awaitTermination()