-- models/silver/intermediate/int_order_lines_enriched.sql
-- Purpose: Calculate line-level amounts and validation flags before silver layer
{{
    config(
        materialized='view'
    )
}}

WITH order_lines AS (
    SELECT * FROM {{ ref('stg_order_lines') }}
),

lines_with_validation AS (
    SELECT
        order_id,
        line_number,
        product_id,
        qty,
        unit_price,
        line_discount_pct,
        tax_pct,
        ingestion_ts,
        
        -- Test: order_id not_null
        CASE WHEN order_id IS NULL THEN TRUE ELSE FALSE END AS has_null_order_id,
        
        -- Test: product_id not_null
        CASE WHEN product_id IS NULL THEN TRUE ELSE FALSE END AS has_null_product_id,
        
        -- Test: discount_percentage_range (0 to 1)
        CASE 
            WHEN line_discount_pct IS NOT NULL 
                AND (line_discount_pct < 0 OR line_discount_pct > 1)
            THEN TRUE 
            ELSE FALSE 
        END AS has_invalid_discount_range,
        
        -- Business logic: quantity validation
        CASE WHEN qty IS NULL OR qty <= 0 THEN TRUE ELSE FALSE END AS has_invalid_quantity,
        
        -- Business logic: unit price validation
        CASE WHEN unit_price IS NULL OR unit_price < 0 THEN TRUE ELSE FALSE END AS has_invalid_unit_price,
        
        -- Relationships (will be validated via staging tests)
        CASE WHEN order_id IS NOT NULL THEN FALSE ELSE FALSE END AS has_invalid_order_relationship,
        CASE WHEN product_id IS NOT NULL THEN FALSE ELSE FALSE END AS has_invalid_product_relationship
        
    FROM order_lines
),

lines_with_amounts AS (
    SELECT
        order_id,
        line_number,
        product_id,
        qty,
        unit_price,
        line_discount_pct,
        tax_pct,
        
        -- Validation flags
        has_null_order_id,
        has_null_product_id,
        has_invalid_discount_range,
        has_invalid_quantity,
        has_invalid_unit_price,
        has_invalid_order_relationship,
        has_invalid_product_relationship,
        
        -- Calculate amounts (only if basic validations pass)
        qty * unit_price AS gross_amount,
        qty * unit_price * COALESCE(line_discount_pct, 0) AS discount_amount,
        qty * unit_price * (1 - COALESCE(line_discount_pct, 0)) AS net_amount,
        qty * unit_price * (1 - COALESCE(line_discount_pct, 0)) * COALESCE(tax_pct, 0) AS tax_amount,
        qty * unit_price * (1 - COALESCE(line_discount_pct, 0)) * (1 + COALESCE(tax_pct, 0)) AS line_total,
        
        -- Overall validity flag
        CASE 
            WHEN order_id IS NULL THEN FALSE
            WHEN product_id IS NULL THEN FALSE
            WHEN line_discount_pct IS NOT NULL AND (line_discount_pct < 0 OR line_discount_pct > 1) THEN FALSE
            WHEN qty IS NULL OR qty <= 0 THEN FALSE
            WHEN unit_price IS NULL OR unit_price < 0 THEN FALSE
            ELSE TRUE
        END AS is_valid_line,
        
        -- Quality issue type
        CASE
            WHEN order_id IS NULL THEN 'Missing Order ID'
            WHEN product_id IS NULL THEN 'Missing Product ID'
            WHEN line_discount_pct IS NOT NULL AND (line_discount_pct < 0 OR line_discount_pct > 1) 
                THEN 'Invalid Discount Range'
            WHEN qty IS NULL OR qty <= 0 THEN 'Invalid Quantity'
            WHEN unit_price IS NULL OR unit_price < 0 THEN 'Invalid Unit Price'
            ELSE NULL
        END AS quality_issue_type,
        
        -- Audit column
        ingestion_ts
        
    FROM lines_with_validation
)

SELECT * FROM lines_with_amounts