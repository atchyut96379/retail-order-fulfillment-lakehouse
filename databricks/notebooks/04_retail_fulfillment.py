# Databricks notebook source
from pyspark.sql import Window, functions as F

raw = "/Volumes/workspace/default/retail_raw"
dates = ("2026-11-20", "2026-11-21")

def read_feed(feed):
    paths = [f"{raw}/batch_date={date}/{feed}.csv" for date in dates]
    return spark.read.option("header", True).csv(paths)

shipments = read_feed("shipments").withColumn(
    "promised_ts_utc", F.expr("try_cast(promised_ts_utc AS TIMESTAMP)")
)

events = (
    read_feed("shipment_events")
    .withColumn("event_ts_utc", F.expr("try_cast(event_ts_utc AS TIMESTAMP)"))
    .withColumn("source_sequence", F.expr("try_cast(source_sequence AS INT)"))
)

print(f"Shipments: {shipments.count()}, events: {events.count()}")

# COMMAND ----------

invalid_events = events.where(
    "event_ts_utc IS NULL OR source_sequence IS NULL OR source_sequence <= 0"
)
if invalid_events.limit(1).count():
    display(invalid_events)
    raise ValueError("Fix the invalid shipment events before publishing this clean batch.")

latest_first = Window.partitionBy("shipment_id").orderBy(
    F.col("source_sequence").desc(),
    F.col("event_ts_utc").desc(),
    F.col("event_id").desc(),
)

latest = (
    events.withColumn("rank", F.row_number().over(latest_first))
    .where("rank = 1")
    .select("shipment_id", F.col("status").alias("current_status"))
)

deliveries = (
    events.where("status = 'DELIVERED'")
    .groupBy("shipment_id")
    .agg(F.min("event_ts_utc").alias("delivered_ts_utc"))
)

# Use a fixed cutoff so reruns give the same result for open shipments.
as_of = F.to_timestamp(F.lit("2026-11-21T23:59:59Z"))

fulfillment = (
    shipments.join(latest, "shipment_id", "left")
    .join(deliveries, "shipment_id", "left")
    .withColumn(
        "delivery_result",
        F.when(F.col("promised_ts_utc").isNull(), "MISSING_PROMISE")
        .when(F.col("delivered_ts_utc") <= F.col("promised_ts_utc"), "ON_TIME")
        .when(F.col("delivered_ts_utc").isNotNull(), "LATE")
        .when(F.col("promised_ts_utc") < as_of, "OPEN_OVERDUE")
        .otherwise("OPEN_NOT_DUE"),
    )
)

display(fulfillment.orderBy("shipment_id"))

# COMMAND ----------

target = "workspace.default.gold_shipment_fulfillment"

(
    fulfillment.withColumn("as_of_ts_utc", as_of)
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable(target)
)

display(
    spark.table(target)
    .groupBy("delivery_result")
    .count()
    .orderBy("delivery_result")
)

# COMMAND ----------

export_path = "/Volumes/workspace/default/retail_exports/shipment_fulfillment/v1"

(
    spark.table("workspace.default.gold_shipment_fulfillment")
    .coalesce(1)
    .write.mode("error")
    .parquet(export_path)
)

print(f"Exported to {export_path}")