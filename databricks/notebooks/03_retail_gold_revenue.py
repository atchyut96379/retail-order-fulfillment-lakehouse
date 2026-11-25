# Databricks notebook source
spark.sql("SET TIME ZONE 'UTC'")

spark.sql("""
CREATE OR REPLACE TABLE workspace.default.gold_daily_revenue
USING DELTA AS
WITH transactions AS (
    SELECT
        CAST(posted_ts_utc AS DATE) AS posting_date,
        amount_inr AS gross_sales_inr,
        CAST(0 AS DECIMAL(18, 2)) AS refunds_inr
    FROM workspace.default.silver_payments
    WHERE status = 'CAPTURED'

    UNION ALL

    SELECT
        CAST(posted_ts_utc AS DATE) AS posting_date,
        CAST(0 AS DECIMAL(18, 2)) AS gross_sales_inr,
        amount_inr AS refunds_inr
    FROM workspace.default.silver_refunds
    WHERE status = 'SUCCEEDED'
)
SELECT
    posting_date,
    SUM(gross_sales_inr) AS gross_sales_inr,
    SUM(refunds_inr) AS refunds_inr,
    SUM(gross_sales_inr) - SUM(refunds_inr) AS net_sales_inr
FROM transactions
GROUP BY posting_date
""")

display(spark.table("workspace.default.gold_daily_revenue").orderBy("posting_date"))

# COMMAND ----------

export_path = "/Volumes/workspace/default/retail_exports/daily_revenue/v1"

(
    spark.table("workspace.default.gold_daily_revenue")
    .coalesce(1)
    .write.mode("overwrite")
    .parquet(export_path)
)

print(f"Parquet export saved to {export_path}")