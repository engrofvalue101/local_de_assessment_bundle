-- models/silver/silver_stores.sql
{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH stores_raw AS (
    SELECT * FROM {{ ref('stg_stores') }}
),

-- Deduplication by store_code
stores_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY store_code 
            ORDER BY ingestion_ts DESC
        ) AS row_num
    FROM stores_raw
),

-- Enrichment
stores_enriched AS (
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
        
        -- Derived: Store age
        CURRENT_DATE - open_dt AS store_age_days,
        
        -- Derived: Store status
        CASE
            WHEN close_dt IS NULL THEN 'Active'
            ELSE 'Closed'
        END AS store_status,
        
        -- Derived: Operational days
        CASE
            WHEN close_dt IS NULL THEN CURRENT_DATE - open_dt
            ELSE close_dt - open_dt
        END AS operational_days,
        
        -- Derived: Store size category (based on region)
        CASE
            WHEN region IN ('Metro', 'Urban') THEN 'Large'
            WHEN region IN ('Suburban') THEN 'Medium'
            WHEN region IN ('Rural') THEN 'Small'
            ELSE 'Unknown'
        END AS store_size_category,
        
        -- Audit
        ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM stores_deduped
    WHERE row_num = 1
)

SELECT * FROM stores_enriched