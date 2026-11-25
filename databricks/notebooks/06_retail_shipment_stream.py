# Databricks notebook source
from pyspark.sql import functions as F

volume = "/Volumes/workspace/default/retail_raw"
input_path = f"{volume}/shipment_stream_input"
checkpoint_path = f"{volume}/shipment_stream_checkpoint_v1"
target = "workspace.default.bronze_shipment_events_stream"

events = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema("""
        event_id STRING,
        shipment_id STRING,
        event_ts_utc STRING,
        source_sequence STRING,
        status STRING
    """)
    .load(input_path)
    .withColumn("source_file", F.col("_metadata.file_name"))
)

query = (
    events.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(target)
)

query.awaitTermination()

result = spark.table(target)
print(f"Total events in Bronze: {result.count()}")
display(result.orderBy("event_id"))

# COMMAND ----------

from pyspark.sql import Window, functions as F

events = spark.table("workspace.default.bronze_shipment_events_stream")
latest_first = Window.partitionBy("shipment_id").orderBy(
    F.col("source_sequence").cast("int").desc(),
    F.col("event_ts_utc").desc(),
    F.col("event_id").desc(),
)

stream_latest = (
    events.withColumn("rank", F.row_number().over(latest_first))
    .where("rank = 1")
    .select(
        "shipment_id",
        F.col("source_sequence").cast("int").alias("stream_sequence"),
        F.col("status").alias("stream_status"),
    )
)

carrier = (
    spark.read.option("header", True)
    .csv("/Volumes/workspace/default/retail_raw/batch_date=2026-11-21/carrier_snapshot.csv")
    .select(
        "shipment_id",
        F.col("source_sequence").cast("int").alias("carrier_sequence"),
        F.col("status").alias("carrier_status"),
    )
)

reconciled = stream_latest.join(carrier, "shipment_id", "full_outer").withColumn(
    "reconciliation_result",
    F.when(
        (F.col("stream_sequence") == F.col("carrier_sequence"))
        & (F.col("stream_status") == F.col("carrier_status")),
        "MATCH",
    ).otherwise("MISMATCH"),
)

display(reconciled.orderBy("shipment_id"))

# COMMAND ----------

stale_carrier = carrier.withColumn(
    "carrier_sequence",
    F.when(F.col("shipment_id") == "SH1", 2)
    .otherwise(F.col("carrier_sequence")),
)

mismatches = stream_latest.join(
    stale_carrier, "shipment_id", "full_outer"
).where(
    ~F.col("stream_sequence").eqNullSafe(F.col("carrier_sequence"))
    | ~F.col("stream_status").eqNullSafe(F.col("carrier_status"))
)

display(mismatches.orderBy("shipment_id"))
assert [row.shipment_id for row in mismatches.select("shipment_id").collect()] == ["SH1"]
print("Mismatch detection passed.")