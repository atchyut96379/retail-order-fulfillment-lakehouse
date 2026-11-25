# Databricks notebook source
from pyspark.sql import Window, functions as F

raw = "/Volumes/workspace/default/retail_raw"
paths = [
    f"{raw}/batch_date=2026-11-20/shipment_events.csv",
    f"{raw}/batch_date=2026-11-21/defect_test/shipment_events.csv",
]

events = (
    spark.read.option("header", True).csv(paths)
    .withColumn("event_time", F.expr("try_cast(event_ts_utc AS TIMESTAMP)"))
    .withColumn("sequence", F.expr("try_cast(source_sequence AS INT)"))
)

# Set aside malformed events and collapse the repeated event ID.
rejected = events.where(F.col("event_time").isNull())
valid = events.where(F.col("event_time").isNotNull())
accepted = valid.dropDuplicates(["event_id"])

# Pick the newest event by source sequence for each shipment.
latest_first = Window.partitionBy("shipment_id").orderBy(
    F.col("sequence").desc(),
    F.col("event_time").desc(),
    F.col("event_id").desc(),
)
latest = (
    accepted.withColumn("rank", F.row_number().over(latest_first))
    .where("rank = 1")
    .select("shipment_id", "event_id", "sequence", "status")
)

counts = {
    "raw": events.count(),
    "duplicates": valid.count() - accepted.count(),
    "rejected": rejected.count(),
    "accepted": accepted.count(),
}
print(counts)
display(rejected.select("event_id", "event_ts_utc"))
display(latest.orderBy("shipment_id"))

assert counts == {"raw": 6, "duplicates": 1, "rejected": 1, "accepted": 4}
assert {row.shipment_id: row.status for row in latest.collect()} == {
    "SH1": "DELIVERED",
    "SH2": "DISPATCHED",
}
print("Shipment quality test passed.")