-- models/gold/dimensions/dim_store.sql
{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH silver_stores AS (
    SELECT * FROM {{ ref('silver_stores') }}
     WHERE is_valid_record = TRUE
),

stores_enhanced AS (
    SELECT
        store_id,
        store_code,
        store_name,
        channel,
        region,
        state,
        latitude,
        longitude,
        open_dt,
        close_dt,
        
        -- Store age
        CURRENT_DATE - open_dt AS store_age_days,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, open_dt)) AS store_age_years,
        
        -- Store status
        CASE
            WHEN close_dt IS NOT NULL THEN 'Closed'
            WHEN CURRENT_DATE - open_dt < 90 THEN 'New'
            WHEN CURRENT_DATE - open_dt < 365 THEN 'Ramping'
            ELSE 'Established'
        END AS operational_status,
        
        -- Geographic region (from region field)
        region AS geographic_region,
        
        -- Climate zone (simplified based on latitude)
        CASE
            WHEN ABS(latitude) < 23.5 THEN 'Tropical'
            WHEN ABS(latitude) < 35 THEN 'Subtropical'
            WHEN ABS(latitude) < 50 THEN 'Temperate'
            ELSE 'Cold'
        END AS climate_zone,
        
        -- Audit columns
        ingestion_ts,
        CURRENT_TIMESTAMP AS gold_updated_at
        
    FROM silver_stores
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} AS store_key,
    *
FROM stores_enhanced
WHERE store_id IS NOT NULL