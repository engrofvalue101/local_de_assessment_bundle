-- models/silver/intermediate/int_order_lines_enriched.sql
-- Purpose: Calculate line-level amounts before aggregating to order level
{{
    config(
        materialized='view'
    )
}}

WITH order_lines AS (
    SELECT * FROM {{ ref('stg_order_lines') }}
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
        
        -- Calculate amounts
        qty * unit_price AS gross_amount,
        qty * unit_price * COALESCE(line_discount_pct, 0) AS discount_amount,
        qty * unit_price * (1 - COALESCE(line_discount_pct, 0)) AS net_amount,
        qty * unit_price * (1 - COALESCE(line_discount_pct, 0)) * COALESCE(tax_pct, 0) AS tax_amount,
        qty * unit_price * (1 - COALESCE(line_discount_pct, 0)) * (1 + COALESCE(tax_pct, 0)) AS line_total,
        
        -- Validation flags
        CASE WHEN qty <= 0 THEN TRUE ELSE FALSE END AS has_invalid_qty,
        CASE WHEN unit_price <= 0 THEN TRUE ELSE FALSE END AS has_invalid_price,
        CASE WHEN COALESCE(line_discount_pct, 0) < 0 OR COALESCE(line_discount_pct, 0) > 1 THEN TRUE ELSE FALSE END AS has_invalid_discount,
        
        -- Overall validity
        CASE 
            WHEN qty > 0 
                AND unit_price > 0
                AND COALESCE(line_discount_pct, 0) BETWEEN 0 AND 1
            THEN TRUE
            ELSE FALSE
        END AS is_valid_line,
        
        -- Audit column
        ingestion_ts
        
    FROM order_lines
)

SELECT * FROM lines_with_amounts