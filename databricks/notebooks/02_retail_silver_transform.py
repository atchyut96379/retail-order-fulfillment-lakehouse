# Databricks notebook source
from pyspark.sql import functions as F

schema = "workspace.default"

orders = spark.table(f"{schema}.bronze_orders").withColumn(
    "order_ts_utc", F.expr("try_cast(order_ts_utc AS TIMESTAMP)")
)

payments = (
    spark.table(f"{schema}.bronze_payments")
    .withColumn("posted_ts_utc", F.expr("try_cast(posted_ts_utc AS TIMESTAMP)"))
    .withColumn("amount_inr", F.expr("try_cast(amount_inr AS DECIMAL(18, 2))"))
)

# COMMAND ----------

def require_no_rows(rows, message):
    if rows.limit(1).count():
        display(rows)
        raise ValueError(message)


require_no_rows(
    orders.where("order_id IS NULL OR trim(order_id) = '' OR order_ts_utc IS NULL"),
    "Orders contain a missing ID or invalid timestamp.",
)
require_no_rows(
    payments.where("""
        payment_id IS NULL OR trim(payment_id) = ''
        OR posted_ts_utc IS NULL OR amount_inr IS NULL OR amount_inr <= 0
    """),
    "Payments contain a missing ID, invalid timestamp, or invalid amount.",
)

for frame, key in ((orders, "order_id"), (payments, "payment_id")):
    require_no_rows(
        frame.groupBy(key).count().where("count > 1"),
        f"Duplicate {key} found in Bronze.",
    )

require_no_rows(
    payments.join(orders.select("order_id"), on="order_id", how="left_anti"),
    "A payment references an order that is missing from Bronze.",
)

for name, frame in (("silver_orders", orders), ("silver_payments", payments)):
    target = f"{schema}.{name}"
    frame.write.format("delta").mode("overwrite").saveAsTable(target)
    print(f"{target}: {spark.table(target).count()} rows")

# COMMAND ----------

from pyspark.sql import functions as F

refunds = (
    spark.table("workspace.default.bronze_refunds")
    .withColumn("posted_ts_utc", F.expr("try_cast(posted_ts_utc AS TIMESTAMP)"))
    .withColumn("amount_inr", F.expr("try_cast(amount_inr AS DECIMAL(18, 2))"))
)

require_no_rows(
    refunds.where("""
        refund_id IS NULL OR trim(refund_id) = ''
        OR posted_ts_utc IS NULL
        OR amount_inr IS NULL OR amount_inr <= 0
    """),
    "A refund has a missing ID, invalid timestamp, or invalid amount.",
)

require_no_rows(
    refunds.groupBy("refund_id").count().where("count > 1"),
    "Duplicate refund ID found.",
)

payment_ids = spark.table("workspace.default.silver_payments").select("payment_id")
require_no_rows(
    refunds.join(payment_ids, on="payment_id", how="left_anti"),
    "A refund references a payment that is missing from Silver.",
)

target = "workspace.default.silver_refunds"
refunds.write.format("delta").mode("overwrite").saveAsTable(target)
display(spark.table(target))