-- Run on the Synapse Built-in serverless SQL endpoint.
-- Replace the storage account host if deploying in another environment.
SELECT shipment_id, order_id, current_status, promised_ts_utc,
       delivered_ts_utc, delivery_result, as_of_ts_utc
FROM OPENROWSET(
    BULK 'https://stretaildev2716.dfs.core.windows.net/retail-gold/shipment_fulfillment/v1/shipment_fulfillment.parquet',
    FORMAT = 'PARQUET'
) AS fulfillment
ORDER BY shipment_id;
