-- models/silver/silver_products.sql
{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH products_raw AS (
    SELECT * FROM {{ ref('stg_products') }}
),

-- Add validation flags based on staging tests
products_with_validation AS (
    SELECT
        *,        
        -- Test: product_id unique and not_null
        CASE WHEN product_id IS NULL THEN TRUE ELSE FALSE END AS has_null_product_id,
        
        -- Test: sku unique and not_null
        CASE WHEN sku IS NULL THEN TRUE ELSE FALSE END AS has_null_sku,
        
        -- Additional business logic validation
        CASE 
            WHEN current_price IS NOT NULL AND current_price < 0 
            THEN TRUE 
            ELSE FALSE 
        END AS has_negative_price,
        
        CASE 
            WHEN discontinued_dt IS NOT NULL AND introduced_dt IS NOT NULL 
                AND discontinued_dt < introduced_dt 
            THEN TRUE 
            ELSE FALSE 
        END AS has_discontinued_before_introduced,
        
        CASE 
            WHEN is_discontinued = TRUE AND discontinued_dt IS NULL 
            THEN TRUE 
            ELSE FALSE 
        END AS has_discontinued_without_date,
        
        -- OVERALL VALIDITY FLAG
        CASE 
            WHEN product_id IS NULL THEN FALSE
            WHEN sku IS NULL THEN FALSE
            WHEN current_price IS NOT NULL AND current_price < 0 THEN FALSE
            WHEN discontinued_dt IS NOT NULL AND introduced_dt IS NOT NULL AND discontinued_dt < introduced_dt THEN FALSE
            WHEN is_discontinued = TRUE AND discontinued_dt IS NULL THEN FALSE
            ELSE TRUE
        END AS is_valid_record,
        
        -- QUALITY ISSUE TYPE
        CASE
            WHEN product_id IS NULL THEN 'Missing Product ID'
            WHEN sku IS NULL THEN 'Missing SKU'
            WHEN current_price IS NOT NULL AND current_price < 0 THEN 'Negative Price'
            WHEN discontinued_dt IS NOT NULL AND introduced_dt IS NOT NULL AND discontinued_dt < introduced_dt 
                THEN 'Discontinued Before Introduced'
            WHEN is_discontinued = TRUE AND discontinued_dt IS NULL THEN 'Discontinued Without Date'
            ELSE NULL
        END AS quality_issue_type
        
    FROM products_raw
),

-- Deduplication by sku (after validation)
products_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY sku 
            ORDER BY ingestion_ts DESC
        ) AS row_num
    FROM products_with_validation
),

-- Enrichment (only for valid products)
products_enriched AS (
    SELECT
        product_id,
        sku,
        product_name,
        category,
        subcategory,
        current_price,
        currency,
        is_discontinued,
        introduced_dt,
        discontinued_dt,
        
        -- Quality flags
        has_null_product_id,
        has_null_sku,
        has_negative_price,
        has_discontinued_before_introduced,
        has_discontinued_without_date,
        is_valid_record,
        quality_issue_type,
        
        -- Derived fields (calculate for all, filter downstream)
        CURRENT_DATE - introduced_dt AS product_age_days,
        
        CASE
            WHEN discontinued_dt IS NOT NULL AND introduced_dt IS NOT NULL
            THEN discontinued_dt - introduced_dt
            ELSE NULL
        END AS product_lifespan_days,
        
        CASE
            WHEN current_price < 10 THEN 'Budget'
            WHEN current_price >= 10 AND current_price < 50 THEN 'Economy'
            WHEN current_price >= 50 AND current_price < 200 THEN 'Mid-Range'
            WHEN current_price >= 200 AND current_price < 1000 THEN 'Premium'
            WHEN current_price >= 1000 THEN 'Luxury'
            ELSE 'Unknown'
        END AS price_tier,
        
        CASE
            WHEN is_discontinued THEN 'Discontinued'
            WHEN CURRENT_DATE - introduced_dt < 90 THEN 'New'
            WHEN CURRENT_DATE - introduced_dt < 365 THEN 'Current'
            WHEN CURRENT_DATE - introduced_dt >= 365 THEN 'Mature'
            ELSE 'Unknown'
        END AS lifecycle_stage,
        
        -- Audit
        ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM products_deduped
    WHERE row_num = 1
)

SELECT * FROM products_enriched