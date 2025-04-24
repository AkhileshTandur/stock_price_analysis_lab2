{{ config(materialized='table') }}

WITH price_deltas AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE,
        CLOSE - LAG(CLOSE) OVER (
            PARTITION BY SYMBOL ORDER BY DATE
        ) AS price_change
    FROM {{ source('raw_data', 'STOCK_PRICE') }}
),
gains_losses AS (
    SELECT
        *,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
    FROM price_deltas
),
avg_gain_loss AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE,
        AVG(gain) OVER (
            PARTITION BY SYMBOL ORDER BY DATE
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain_14d,
        AVG(loss) OVER (
            PARTITION BY SYMBOL ORDER BY DATE
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_loss_14d
    FROM gains_losses
)
SELECT
    SYMBOL,
    DATE,
    CLOSE,
    CASE 
        WHEN avg_loss_14d = 0 THEN 100
        ELSE 100 - (100 / (1 + (avg_gain_14d / NULLIF(avg_loss_14d,0))))
    END AS rsi_14d
FROM avg_gain_loss
WHERE DATE IS NOT NULL
