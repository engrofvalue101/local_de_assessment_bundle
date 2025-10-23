-- models/gold/dimensions/dim_product_scd.sql
{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH product_snapshot AS (
    SELECT * FROM {{ ref('products_snapshot') }}
     WHERE is_valid_record = TRUE
),

products_with_changes AS (
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
        
        -- SCD Type 2 columns from snapshot
        dbt_valid_from,
        dbt_valid_to,
        dbt_scd_id,
        
        -- Flag for current record
        CASE 
            WHEN dbt_valid_to IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_current,
        
        -- Calculate days this version was active
        CASE
            WHEN dbt_valid_to IS NULL 
            THEN CURRENT_DATE - dbt_valid_from::DATE
            ELSE dbt_valid_to::DATE - dbt_valid_from::DATE
        END AS version_days_active,
        
        -- Price change indicators
        LAG(current_price) OVER (
            PARTITION BY product_id 
            ORDER BY dbt_valid_from
        ) AS previous_price,
        
        -- Version number per product
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY dbt_valid_from
        ) AS version_number
        
    FROM product_snapshot
),

enhanced_products AS (
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
        dbt_valid_from,
        dbt_valid_to,
        dbt_scd_id,
        is_current,
        version_days_active,
        version_number,
        previous_price,
        
        -- Price change analysis
        CASE
            WHEN previous_price IS NULL THEN NULL
            WHEN current_price > previous_price THEN 'Increased'
            WHEN current_price < previous_price THEN 'Decreased'
            ELSE 'Unchanged'
        END AS price_change_type,
        
        CASE
            WHEN previous_price IS NOT NULL AND previous_price > 0
            THEN ((current_price - previous_price) / previous_price) * 100
            ELSE NULL
        END AS price_change_pct,
        
        -- Product categorization
        CASE
            WHEN current_price < 50 THEN 'Low'
            WHEN current_price < 200 THEN 'Medium'
            WHEN current_price < 500 THEN 'High'
            ELSE 'Premium'
        END AS price_tier,
        
        -- Status indicators
        CASE
            WHEN is_discontinued = TRUE THEN 'Discontinued'
            WHEN is_current = FALSE THEN 'Historical'
            ELSE 'Active'
        END AS product_status,
        
        -- Audit
        CURRENT_TIMESTAMP AS gold_updated_at
        
    FROM products_with_changes
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['dbt_scd_id']) }} AS product_key,
    *
FROM enhanced_products
WHERE product_id IS NOT NULL