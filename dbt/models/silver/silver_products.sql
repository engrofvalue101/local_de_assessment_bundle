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

-- Deduplication by sku
products_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY sku 
            ORDER BY ingestion_ts DESC
        ) AS row_num
    FROM products_raw
),

-- Enrichment
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
        
        -- Derived: Product age
        CURRENT_DATE - introduced_dt AS product_age_days,
        
        -- Derived: Product lifespan (for discontinued products)
        CASE
            WHEN discontinued_dt IS NOT NULL AND introduced_dt IS NOT NULL
            THEN discontinued_dt - introduced_dt
            ELSE NULL
        END AS product_lifespan_days,
        
        -- Derived: Price tier
        CASE
            WHEN current_price < 10 THEN 'Budget'
            WHEN current_price >= 10 AND current_price < 50 THEN 'Economy'
            WHEN current_price >= 50 AND current_price < 200 THEN 'Mid-Range'
            WHEN current_price >= 200 AND current_price < 1000 THEN 'Premium'
            WHEN current_price >= 1000 THEN 'Luxury'
            ELSE 'Unknown'
        END AS price_tier,
        
        -- Derived: Lifecycle stage
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