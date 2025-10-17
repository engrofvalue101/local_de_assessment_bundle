-- models/silver/silver_order_lines.sql
{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'line_number'],
        on_schema_change='merge',
        schema='silver'
    )
}}

WITH order_lines_enriched AS (
    SELECT * FROM {{ ref('int_order_lines_enriched') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
),

-- Get order context
orders AS (
    SELECT 
        order_id,
        customer_id,
        store_id,
        order_ts,
        order_dt_local,
        channel,
        currency
    FROM {{ ref('silver_orders') }}
),

-- Get product context
products AS (
    SELECT
        product_id,
        category,
        subcategory,
        price_tier
    FROM {{ ref('silver_products') }}
),

-- Final enrichment
lines_final AS (
    SELECT
        ol.order_id,
        ol.line_number,
        ol.product_id,
        o.customer_id,
        o.store_id,
        o.order_ts,
        o.order_dt_local,
        o.channel,
        o.currency,
        
        -- Line details
        ol.qty,
        ol.unit_price,
        ol.line_discount_pct,
        ol.tax_pct,
        
        -- Calculated amounts
        ol.gross_amount,
        ol.discount_amount,
        ol.net_amount,
        ol.tax_amount,
        ol.line_total,
        
        -- Product information
        p.category,
        p.subcategory,
        p.price_tier,
        
        -- Derived: Discount band
        CASE
            WHEN ol.line_discount_pct IS NULL OR ol.line_discount_pct = 0 THEN 'No Discount'
            WHEN ol.line_discount_pct > 0 AND ol.line_discount_pct <= 0.1 THEN '1-10%'
            WHEN ol.line_discount_pct > 0.1 AND ol.line_discount_pct <= 0.25 THEN '11-25%'
            WHEN ol.line_discount_pct > 0.25 AND ol.line_discount_pct <= 0.5 THEN '26-50%'
            WHEN ol.line_discount_pct > 0.5 THEN 'Over 50%'
            ELSE 'Unknown'
        END AS discount_band,
        
        -- Derived: Quantity band
        CASE
            WHEN ol.qty = 1 THEN 'Single Unit'
            WHEN ol.qty BETWEEN 2 AND 5 THEN 'Small Batch'
            WHEN ol.qty BETWEEN 6 AND 10 THEN 'Medium Batch'
            WHEN ol.qty > 10 THEN 'Bulk Order'
            ELSE 'Unknown'
        END AS quantity_band,
        
        -- Audit
        ol.ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM order_lines_enriched ol
    INNER JOIN orders o ON ol.order_id = o.order_id
    LEFT JOIN products p ON ol.product_id = p.product_id
    WHERE ol.is_valid_line = TRUE
)

SELECT * FROM lines_final