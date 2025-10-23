-- models/silver/silver_order_lines.sql
{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'line_number'],
        on_schema_change='merge',
        schema='silver'
    )
}}

WITH order_lines_raw AS (
    SELECT * FROM {{ ref('int_order_lines_enriched') }}
    {% if is_incremental() %}
    WHERE ingestion_ts > (
        SELECT COALESCE(MAX(ingestion_ts), '1900-01-01'::TIMESTAMP) 
        FROM {{ this }}
    )
    {% endif %}
),
-- Get order context
order_lines_with_validation AS (
    SELECT
        *,
        
        CASE WHEN order_id IS NULL THEN TRUE ELSE FALSE END AS has_null_order_id,
        CASE WHEN product_id IS NULL THEN TRUE ELSE FALSE END AS has_null_product_id,
        
        -- Discount percentage range check (0 to 1)
        CASE 
            WHEN line_discount_pct IS NOT NULL 
                AND (line_discount_pct < 0 OR line_discount_pct > 1)
            THEN TRUE
            ELSE FALSE
        END AS has_invalid_discount_range,
        
        -- Relationship checks (will be validated via staging tests)
        CASE WHEN order_id IS NOT NULL THEN FALSE ELSE FALSE END AS has_invalid_order_relationship,
        CASE WHEN product_id IS NOT NULL THEN FALSE ELSE FALSE END AS has_invalid_product_relationship,
        
        -- Additional quality checks
        CASE WHEN qty IS NULL OR qty <= 0 THEN TRUE ELSE FALSE END AS has_invalid_quantity,
        CASE WHEN unit_price IS NULL OR unit_price < 0 THEN TRUE ELSE FALSE END AS has_invalid_unit_price,
        
        -- QUALITY ISSUE TYPE (update existing or add new)
        CASE
            WHEN order_id IS NULL THEN 'Missing Order ID'
            WHEN product_id IS NULL THEN 'Missing Product ID'
            WHEN line_discount_pct IS NOT NULL AND (line_discount_pct < 0 OR line_discount_pct > 1) 
                THEN 'Invalid Discount Range'
            WHEN qty IS NULL OR qty <= 0 THEN 'Invalid Quantity'
            WHEN unit_price IS NULL OR unit_price < 0 THEN 'Invalid Unit Price'
            ELSE NULL
        END AS additional_quality_issue
        
    FROM order_lines_raw
),

-- Combine quality issues
order_lines_validated AS (
    SELECT
        *,
        -- Combine quality issues if both exist
        CASE 
            WHEN quality_issue_type IS NOT NULL AND additional_quality_issue IS NOT NULL 
                THEN quality_issue_type || '; ' || additional_quality_issue
            WHEN quality_issue_type IS NOT NULL THEN quality_issue_type
            WHEN additional_quality_issue IS NOT NULL THEN additional_quality_issue
            ELSE NULL
        END AS combined_quality_issue,
        
        -- Update overall validity flag
        CASE 
            WHEN is_valid_line = FALSE THEN FALSE  -- Already marked invalid
            WHEN order_id IS NULL THEN FALSE
            WHEN product_id IS NULL THEN FALSE
            WHEN line_discount_pct IS NOT NULL AND (line_discount_pct < 0 OR line_discount_pct > 1) THEN FALSE
            WHEN qty IS NULL OR qty <= 0 THEN FALSE
            WHEN unit_price IS NULL OR unit_price < 0 THEN FALSE
            ELSE TRUE
        END AS is_valid_record
        
    FROM order_lines_with_validation
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
    WHERE is_valid_record = TRUE  -- Only join to valid orders
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
        
        -- Quality flags
        ol.has_null_order_id,
        ol.has_null_product_id,
        ol.has_invalid_discount_range,
        ol.has_invalid_order_relationship,
        ol.has_invalid_product_relationship,
        ol.has_invalid_quantity,
        ol.has_invalid_unit_price,
        ol.is_valid_record,
        ol.combined_quality_issue AS quality_issue_type,
        
        -- Derived fields
        CASE
            WHEN ol.line_discount_pct IS NULL OR ol.line_discount_pct = 0 THEN 'No Discount'
            WHEN ol.line_discount_pct > 0 AND ol.line_discount_pct <= 0.1 THEN '1-10%'
            WHEN ol.line_discount_pct > 0.1 AND ol.line_discount_pct <= 0.25 THEN '11-25%'
            WHEN ol.line_discount_pct > 0.25 AND ol.line_discount_pct <= 0.5 THEN '26-50%'
            WHEN ol.line_discount_pct > 0.5 THEN 'Over 50%'
            ELSE 'Unknown'
        END AS discount_band,
        
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
        
    FROM order_lines_validated ol
    INNER JOIN orders o ON ol.order_id = o.order_id
    LEFT JOIN products p ON ol.product_id = p.product_id
)

SELECT * FROM lines_final