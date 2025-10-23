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

-- Add validation flags based on staging tests
stores_with_validation AS (
    SELECT
        *,
        
        -- Test: store_id unique and not_null
        CASE WHEN store_id IS NULL THEN TRUE ELSE FALSE END AS has_null_store_id,
        
        -- Test: store_code unique and not_null
        CASE WHEN store_code IS NULL THEN TRUE ELSE FALSE END AS has_null_store_code,
        
        -- Additional business logic validation
        CASE 
            WHEN latitude IS NOT NULL AND (latitude < -90 OR latitude > 90)
            THEN TRUE 
            ELSE FALSE 
        END AS has_invalid_latitude_range,
        
        CASE 
            WHEN longitude IS NOT NULL AND (longitude < -180 OR longitude > 180)
            THEN TRUE 
            ELSE FALSE 
        END AS has_invalid_longitude_range,
        
        CASE 
            WHEN close_dt IS NOT NULL AND open_dt IS NOT NULL 
                AND close_dt < open_dt 
            THEN TRUE 
            ELSE FALSE 
        END AS has_closed_before_opened,
        
        CASE 
            WHEN open_dt > CURRENT_DATE 
            THEN TRUE 
            ELSE FALSE 
        END AS has_future_open_date,
        
        -- OVERALL VALIDITY FLAG
        CASE 
            WHEN store_id IS NULL THEN FALSE
            WHEN store_code IS NULL THEN FALSE
            WHEN latitude IS NOT NULL AND (latitude < -90 OR latitude > 90) THEN FALSE
            WHEN longitude IS NOT NULL AND (longitude < -180 OR longitude > 180) THEN FALSE
            WHEN close_dt IS NOT NULL AND open_dt IS NOT NULL AND close_dt < open_dt THEN FALSE
            WHEN open_dt > CURRENT_DATE THEN FALSE
            ELSE TRUE
        END AS is_valid_record,
        
        -- QUALITY ISSUE TYPE
        CASE
            WHEN store_id IS NULL THEN 'Missing Store ID'
            WHEN store_code IS NULL THEN 'Missing Store Code'
            WHEN latitude IS NOT NULL AND (latitude < -90 OR latitude > 90) THEN 'Invalid Latitude Range'
            WHEN longitude IS NOT NULL AND (longitude < -180 OR longitude > 180) THEN 'Invalid Longitude Range'
            WHEN close_dt IS NOT NULL AND open_dt IS NOT NULL AND close_dt < open_dt THEN 'Closed Before Opened'
            WHEN open_dt > CURRENT_DATE THEN 'Future Open Date'
            ELSE NULL
        END AS quality_issue_type
        
    FROM stores_raw
),

-- Deduplication by store_code (after validation)
stores_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY store_code 
            ORDER BY ingestion_ts DESC
        ) AS row_num
    FROM stores_with_validation
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
        
        -- Quality flags
        has_null_store_id,
        has_null_store_code,
        has_invalid_latitude_range,
        has_invalid_longitude_range,
        has_closed_before_opened,
        has_future_open_date,
        is_valid_record,
        quality_issue_type,
        
        -- Derived fields
        CURRENT_DATE - open_dt AS store_age_days,
        
        CASE
            WHEN close_dt IS NULL THEN 'Active'
            ELSE 'Closed'
        END AS store_status,
        
        CASE
            WHEN close_dt IS NULL THEN CURRENT_DATE - open_dt
            ELSE close_dt - open_dt
        END AS operational_days,
        
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