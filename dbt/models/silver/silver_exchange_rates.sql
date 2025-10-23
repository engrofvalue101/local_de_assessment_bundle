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

-- Add validation flags based on business rules
rates_with_validation AS (
    SELECT
        *,
        
        -- Basic null checks
        CASE WHEN rate_date IS NULL THEN TRUE ELSE FALSE END AS has_null_rate_date,
        CASE WHEN currency IS NULL THEN TRUE ELSE FALSE END AS has_null_currency,
        CASE WHEN rate_to_aud IS NULL THEN TRUE ELSE FALSE END AS has_null_rate,
        
        -- Rate validation
        CASE 
            WHEN rate_to_aud IS NOT NULL AND rate_to_aud <= 0 
            THEN TRUE 
            ELSE FALSE 
        END AS has_zero_or_negative_rate,
        
        CASE 
            WHEN rate_to_aud IS NOT NULL AND rate_to_aud > 1000 
            THEN TRUE 
            ELSE FALSE 
        END AS has_extreme_rate,
        
        -- Future date check
        CASE 
            WHEN rate_date > CURRENT_DATE 
            THEN TRUE 
            ELSE FALSE 
        END AS has_future_rate_date,
        
        -- Very old rate (more than 10 years old)
        CASE 
            WHEN rate_date < CURRENT_DATE - INTERVAL '10 years' 
            THEN TRUE 
            ELSE FALSE 
        END AS has_very_old_rate,
        
        -- OVERALL VALIDITY FLAG
        CASE 
            WHEN rate_date IS NULL THEN FALSE
            WHEN currency IS NULL THEN FALSE
            WHEN rate_to_aud IS NULL THEN FALSE
            WHEN rate_to_aud <= 0 THEN FALSE
            WHEN rate_to_aud > 1000 THEN FALSE
            WHEN rate_date > CURRENT_DATE THEN FALSE
            ELSE TRUE
        END AS is_valid_record,
        
        -- QUALITY ISSUE TYPE
        CASE
            WHEN rate_date IS NULL THEN 'Missing Rate Date'
            WHEN currency IS NULL THEN 'Missing Currency'
            WHEN rate_to_aud IS NULL THEN 'Missing Exchange Rate'
            WHEN rate_to_aud <= 0 THEN 'Zero or Negative Rate'
            WHEN rate_to_aud > 1000 THEN 'Extreme Rate (>1000)'
            WHEN rate_date > CURRENT_DATE THEN 'Future Rate Date'
            ELSE NULL
        END AS quality_issue_type
        
    FROM rates_raw
),

-- Deduplication (should not have duplicates, but safety check)
rates_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY rate_date, currency 
            ORDER BY ingestion_ts DESC
        ) AS row_num
    FROM rates_with_validation
),

-- Enrichment
rates_enriched AS (
    SELECT
        rate_date,
        currency,
        rate_to_aud,
        
        -- Quality flags
        has_null_rate_date,
        has_null_currency,
        has_null_rate,
        has_zero_or_negative_rate,
        has_extreme_rate,
        has_future_rate_date,
        has_very_old_rate,
        is_valid_record,
        quality_issue_type,
        
        -- Derived fields
        CASE 
            WHEN rate_to_aud > 0 THEN 1.0 / rate_to_aud 
            ELSE NULL 
        END AS aud_to_currency_rate,
        
        EXTRACT(DOW FROM rate_date) AS day_of_week,
        
        CASE 
            WHEN EXTRACT(DOW FROM rate_date) BETWEEN 1 AND 5 THEN TRUE 
            ELSE FALSE 
        END AS is_business_day,
        
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