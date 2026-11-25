# Databricks notebook source
from pyspark.sql import functions as F

raw_volume = "/Volumes/workspace/default/retail_raw"
batch_date = "2026-11-20"

for feed in ("orders", "payments"):
    source_path = f"{raw_volume}/batch_date={batch_date}/{feed}.csv"
    target_table = f"workspace.default.bronze_{feed}"

    bronze = (
        spark.read.option("header", True)
        .csv(source_path)
        .withColumn("batch_date", F.to_date(F.lit(batch_date)))
        .withColumn("source_file", F.lit(source_path))
        .withColumn("ingested_at", F.current_timestamp())
    )

    if spark.catalog.tableExists(target_table):
        (
            bronze.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"batch_date = DATE '{batch_date}'")
            .saveAsTable(target_table)
        )
    else:
        bronze.write.format("delta").saveAsTable(target_table)

    print(target_table)
    display(spark.table(target_table).where(F.col("batch_date") == F.lit(batch_date)))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'orders' AS feed, batch_date, COUNT(*) AS row_count
# MAGIC FROM workspace.default.bronze_orders
# MAGIC GROUP BY batch_date
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'payments' AS feed, batch_date, COUNT(*) AS row_count
# MAGIC FROM workspace.default.bronze_payments
# MAGIC GROUP BY batch_date
# MAGIC
# MAGIC ORDER BY feed, batch_date;

# COMMAND ----------

from pyspark.sql import functions as F

refund_date = "2026-11-21"
refund_path = (
    f"/Volumes/workspace/default/retail_raw/batch_date={refund_date}/refunds.csv"
)
target = "workspace.default.bronze_refunds"

refunds_bronze = (
    spark.read.option("header", True).csv(refund_path)
    .withColumn("batch_date", F.to_date(F.lit(refund_date)))
    .withColumn("source_file", F.lit(refund_path))
    .withColumn("ingested_at", F.current_timestamp())
)

writer = refunds_bronze.write.format("delta")
if spark.catalog.tableExists(target):
    writer.mode("overwrite").option(
        "replaceWhere", f"batch_date = DATE '{refund_date}'"
    ).saveAsTable(target)
else:
    writer.saveAsTable(target)

display(spark.table(target).where(F.col("batch_date") == refund_date))