-- models/silver/silver_exchange_rates.sql
{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH rates_raw AS (
    SELECT * FROM {{ ref('stg_exchange_rates') }}
),

-- Deduplication (should not have duplicates, but safety check)
rates_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY rate_date, currency 
            ORDER BY ingestion_ts DESC
        ) AS row_num
    FROM rates_raw
),

-- Enrichment
rates_enriched AS (
    SELECT
        rate_date,
        currency,
        rate_to_aud,
        
        -- Derived: Inverse rate
        CASE 
            WHEN rate_to_aud > 0 THEN 1.0 / rate_to_aud 
            ELSE NULL 
        END AS aud_to_currency_rate,
        
        -- Derived: Day of week
        EXTRACT(DOW FROM rate_date) AS day_of_week,
        
        -- Derived: Is business day
        CASE 
            WHEN EXTRACT(DOW FROM rate_date) BETWEEN 1 AND 5 THEN TRUE 
            ELSE FALSE 
        END AS is_business_day,
        
        -- Derived: Month and year
        EXTRACT(YEAR FROM rate_date) AS rate_year,
        EXTRACT(MONTH FROM rate_date) AS rate_month,
        
        -- Audit
        ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM rates_deduped
    WHERE row_num = 1
),

-- Calculate rate changes (using window functions)
rates_with_changes AS (
    SELECT
        *,
        LAG(rate_to_aud) OVER (PARTITION BY currency ORDER BY rate_date) AS prev_day_rate,
        
        CASE
            WHEN LAG(rate_to_aud) OVER (PARTITION BY currency ORDER BY rate_date) IS NOT NULL
            THEN rate_to_aud - LAG(rate_to_aud) OVER (PARTITION BY currency ORDER BY rate_date)
            ELSE NULL
        END AS rate_change,
        
        CASE
            WHEN LAG(rate_to_aud) OVER (PARTITION BY currency ORDER BY rate_date) IS NOT NULL
                AND LAG(rate_to_aud) OVER (PARTITION BY currency ORDER BY rate_date) > 0
            THEN ((rate_to_aud - LAG(rate_to_aud) OVER (PARTITION BY currency ORDER BY rate_date)) 
                  / LAG(rate_to_aud) OVER (PARTITION BY currency ORDER BY rate_date)) * 100
            ELSE NULL
        END AS rate_change_pct
        
    FROM rates_enriched
)

SELECT * FROM rates_with_changes