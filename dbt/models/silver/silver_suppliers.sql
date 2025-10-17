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

-- Deduplication by supplier_code
suppliers_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY supplier_code 
            ORDER BY ingestion_ts DESC
        ) AS row_num
    FROM suppliers_raw
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
        
        -- Derived: Lead time category
        CASE
            WHEN lead_time_days <= 7 THEN 'Express'
            WHEN lead_time_days > 7 AND lead_time_days <= 14 THEN 'Standard'
            WHEN lead_time_days > 14 AND lead_time_days <= 30 THEN 'Extended'
            WHEN lead_time_days > 30 THEN 'Long Lead'
            ELSE 'Unknown'
        END AS lead_time_category,
        
        -- Derived: Supplier tier (based on preferred status and lead time)
        CASE
            WHEN preferred AND lead_time_days <= 7 THEN 'Tier 1 - Premium'
            WHEN preferred AND lead_time_days <= 14 THEN 'Tier 2 - Preferred'
            WHEN preferred THEN 'Tier 3 - Standard Preferred'
            WHEN lead_time_days <= 7 THEN 'Tier 4 - Fast'
            ELSE 'Tier 5 - Standard'
        END AS supplier_tier,
        
        -- Derived: Supplier region
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