{{ config(materialized='table') }}

SELECT
    SYMBOL,
    DATE,
    CLOSE,
    AVG(CLOSE) OVER (
        PARTITION BY SYMBOL
        ORDER BY DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d
FROM {{ source('raw_data', 'STOCK_PRICE') }}
WHERE DATE IS NOT NULL
