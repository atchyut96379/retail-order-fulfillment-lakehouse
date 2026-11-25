-- Run on the Synapse Built-in serverless SQL endpoint.
-- Replace the storage account host if deploying in another environment.
SELECT posting_date, gross_sales_inr, refunds_inr, net_sales_inr
FROM OPENROWSET(
    BULK 'https://stretaildev2716.dfs.core.windows.net/retail-gold/daily_revenue/v1/daily_revenue.parquet',
    FORMAT = 'PARQUET'
) AS revenue
ORDER BY posting_date;
