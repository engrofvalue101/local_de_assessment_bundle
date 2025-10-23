-- models/gold/dimensions/dim_supplier.sql
{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH silver_suppliers AS (
    SELECT * FROM {{ ref('silver_suppliers') }}
     WHERE is_valid_record = TRUE
),

suppliers_enhanced AS (
    SELECT
        supplier_id,
        supplier_code,
        supplier_name,
        country_code,
        lead_time_days,
        preferred,
        
        -- Supplier tier based on lead time
        CASE
            WHEN lead_time_days <= 3 THEN 'Tier 1 - Express'
            WHEN lead_time_days <= 7 THEN 'Tier 2 - Fast'
            WHEN lead_time_days <= 14 THEN 'Tier 3 - Standard'
            ELSE 'Tier 4 - Slow'
        END AS supplier_tier,
        
        -- Geographic region mapping
        CASE
            WHEN country_code IN ('US', 'CA', 'MX') THEN 'North America'
            WHEN country_code IN ('GB', 'DE', 'FR', 'IT', 'ES', 'NL') THEN 'Europe'
            WHEN country_code IN ('CN', 'JP', 'KR', 'TW', 'SG') THEN 'Asia Pacific'
            WHEN country_code IN ('AU', 'NZ') THEN 'Oceania'
            ELSE 'Other'
        END AS supplier_region,
        
        -- Lead time category
        CASE
            WHEN lead_time_days <= 7 THEN 'Fast'
            WHEN lead_time_days <= 21 THEN 'Medium'
            ELSE 'Slow'
        END AS lead_time_category,
        
        -- Preferred status label
        CASE
            WHEN preferred = TRUE THEN 'Preferred'
            ELSE 'Standard'
        END AS supplier_status,
        
        -- Audit columns
        ingestion_ts,
        CURRENT_TIMESTAMP AS gold_updated_at
        
    FROM silver_suppliers
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} AS supplier_key,
    *
FROM suppliers_enhanced
WHERE supplier_id IS NOT NULL