-- models/silver/silver_suppliers.sql
{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH suppliers_raw AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
),

-- Add validation flags based on staging tests
suppliers_with_validation AS (
    SELECT
        *,        
        -- Test: supplier_id unique and not_null
        CASE WHEN supplier_id IS NULL THEN TRUE ELSE FALSE END AS has_null_supplier_id,
        
        -- Test: supplier_code unique and not_null
        CASE WHEN supplier_code IS NULL THEN TRUE ELSE FALSE END AS has_null_supplier_code,
        
        -- Additional business logic validation
        CASE 
            WHEN lead_time_days IS NOT NULL AND lead_time_days < 0 
            THEN TRUE 
            ELSE FALSE 
        END AS has_negative_lead_time,
        
        CASE 
            WHEN lead_time_days IS NOT NULL AND lead_time_days > 365 
            THEN TRUE 
            ELSE FALSE 
        END AS has_extreme_lead_time,
        
        CASE 
            WHEN country_code IS NULL 
            THEN TRUE 
            ELSE FALSE 
        END AS has_missing_country_code,
        
        -- OVERALL VALIDITY FLAG
        CASE 
            WHEN supplier_id IS NULL THEN FALSE
            WHEN supplier_code IS NULL THEN FALSE
            WHEN lead_time_days IS NOT NULL AND lead_time_days < 0 THEN FALSE
            WHEN lead_time_days IS NOT NULL AND lead_time_days > 365 THEN FALSE
            WHEN country_code IS NULL THEN FALSE
            ELSE TRUE
        END AS is_valid_record,
        
        -- QUALITY ISSUE TYPE
        CASE
            WHEN supplier_id IS NULL THEN 'Missing Supplier ID'
            WHEN supplier_code IS NULL THEN 'Missing Supplier Code'
            WHEN lead_time_days IS NOT NULL AND lead_time_days < 0 THEN 'Negative Lead Time'
            WHEN lead_time_days IS NOT NULL AND lead_time_days > 365 THEN 'Extreme Lead Time (>365 days)'
            WHEN country_code IS NULL THEN 'Missing Country Code'
            ELSE NULL
        END AS quality_issue_type
        
    FROM suppliers_raw
),

-- Deduplication by supplier_code (after validation)
suppliers_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY supplier_code 
            ORDER BY ingestion_ts DESC
        ) AS row_num
    FROM suppliers_with_validation
),

-- Enrichment
suppliers_enriched AS (
    SELECT
        supplier_id,
        supplier_code,
        supplier_name,
        country_code,
        lead_time_days,
        preferred,
        
        -- Quality flags
        has_null_supplier_id,
        has_null_supplier_code,
        has_negative_lead_time,
        has_extreme_lead_time,
        has_missing_country_code,
        is_valid_record,
        quality_issue_type,
        
        -- Derived fields
        CASE
            WHEN lead_time_days <= 7 THEN 'Express'
            WHEN lead_time_days > 7 AND lead_time_days <= 14 THEN 'Standard'
            WHEN lead_time_days > 14 AND lead_time_days <= 30 THEN 'Extended'
            WHEN lead_time_days > 30 THEN 'Long Lead'
            ELSE 'Unknown'
        END AS lead_time_category,
        
        CASE
            WHEN preferred AND lead_time_days <= 7 THEN 'Tier 1 - Premium'
            WHEN preferred AND lead_time_days <= 14 THEN 'Tier 2 - Preferred'
            WHEN preferred THEN 'Tier 3 - Standard Preferred'
            WHEN lead_time_days <= 7 THEN 'Tier 4 - Fast'
            ELSE 'Tier 5 - Standard'
        END AS supplier_tier,
        
        CASE
            WHEN country_code IN ('AU', 'NZ') THEN 'Oceania'
            WHEN country_code IN ('US', 'CA', 'MX') THEN 'North America'
            WHEN country_code IN ('GB', 'DE', 'FR', 'IT', 'ES') THEN 'Europe'
            WHEN country_code IN ('CN', 'JP', 'KR', 'SG') THEN 'Asia'
            ELSE 'Other'
        END AS supplier_region,
        
        -- Audit
        ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM suppliers_deduped
    WHERE row_num = 1
)

SELECT * FROM suppliers_enriched