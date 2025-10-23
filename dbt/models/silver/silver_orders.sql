-- models/silver/silver_orders.sql
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='merge',
        schema='silver'
    )
}}

WITH orders_raw AS (
    SELECT * FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
    WHERE ingestion_ts > (
        SELECT COALESCE(MAX(ingestion_ts), '1900-01-01'::TIMESTAMP) 
        FROM {{ this }}
    )
    {% endif %}
),
-- Deduplication by order_id
orders_with_validation AS (
    SELECT
        *,
        
        -- VALIDATION FLAGS
        CASE WHEN order_id IS NULL THEN TRUE ELSE FALSE END AS has_null_order_id,
        CASE WHEN customer_id IS NULL THEN TRUE ELSE FALSE END AS has_null_customer_id,
        CASE WHEN order_ts IS NULL THEN TRUE ELSE FALSE END AS has_null_order_ts,
        
        -- Future order check
        CASE WHEN order_ts > CURRENT_TIMESTAMP THEN TRUE ELSE FALSE END AS has_future_order_ts,
        
        -- Relationships will be validated via staging tests
        CASE WHEN customer_id IS NOT NULL THEN FALSE ELSE FALSE END AS has_invalid_customer_relationship,
        CASE WHEN store_id IS NOT NULL THEN FALSE ELSE FALSE END AS has_invalid_store_relationship,
        
        -- OVERALL VALIDITY FLAG
        CASE 
            WHEN order_id IS NULL THEN FALSE
            WHEN customer_id IS NULL THEN FALSE
            WHEN order_ts IS NULL THEN FALSE
            WHEN order_ts > CURRENT_TIMESTAMP THEN FALSE
            ELSE TRUE
        END AS is_valid_record,
        
        -- QUALITY ISSUE TYPE
        CASE
            WHEN order_id IS NULL THEN 'Missing Order ID'
            WHEN customer_id IS NULL THEN 'Missing Customer ID'
            WHEN order_ts IS NULL THEN 'Missing Order Timestamp'
            WHEN order_ts > CURRENT_TIMESTAMP THEN 'Future Order Timestamp'
            ELSE NULL
        END AS quality_issue_type
        
    FROM orders_raw
),

-- Deduplication by order_id
orders_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY ingestion_ts DESC
        ) AS row_num
    FROM orders_with_validation
),

orders_base AS (
    SELECT
        order_id,
        customer_id,
        store_id,
        order_ts,
        order_dt_local,
        channel,
        payment_method,
        coupon_code,
        shipping_fee,
        currency,
        
        -- Quality flags
        has_null_order_id,
        has_null_customer_id,
        has_null_order_ts,
        has_future_order_ts,
        has_invalid_customer_relationship,
        has_invalid_store_relationship,
        is_valid_record,
        quality_issue_type,
        
        ingestion_ts,
        src_filename,
        src_row_hash
    FROM orders_deduped
    WHERE row_num = 1
),

-- Aggregate order lines to get order totals
order_line_aggregates AS (
    SELECT
        order_id,
        COUNT(*) AS line_count,
        COUNT(DISTINCT product_id) AS unique_product_count,
        SUM(qty) AS total_quantity,
        SUM(gross_amount) AS order_gross_amount,
        SUM(discount_amount) AS order_discount_amount,
        SUM(net_amount) AS order_net_amount,
        SUM(tax_amount) AS order_tax_amount,
        SUM(line_total) AS order_subtotal
    FROM {{ ref('int_order_lines_enriched') }}
    WHERE is_valid_line = TRUE
    GROUP BY order_id
),

-- Enrichment
orders_enriched AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.store_id,
        o.order_ts,
        o.order_dt_local,
        o.channel,
        o.payment_method,
        o.coupon_code,
        o.shipping_fee,
        o.currency,
        
        -- Quality flags
        o.has_null_order_id,
        o.has_null_customer_id,
        o.has_null_order_ts,
        o.has_future_order_ts,
        o.has_invalid_customer_relationship,
        o.has_invalid_store_relationship,
        o.is_valid_record,
        o.quality_issue_type,
        
        -- Aggregated amounts from order lines
        COALESCE(ola.line_count, 0) AS line_count,
        COALESCE(ola.unique_product_count, 0) AS unique_product_count,
        COALESCE(ola.total_quantity, 0) AS total_quantity,
        COALESCE(ola.order_gross_amount, 0) AS order_gross_amount,
        COALESCE(ola.order_discount_amount, 0) AS order_discount_amount,
        COALESCE(ola.order_net_amount, 0) AS order_net_amount,
        COALESCE(ola.order_tax_amount, 0) AS order_tax_amount,
        COALESCE(ola.order_subtotal, 0) AS order_subtotal,
        COALESCE(ola.order_subtotal, 0) + COALESCE(o.shipping_fee, 0) AS order_total,
        
        -- Derived fields (calculate for all, filter downstream)
        EXTRACT(HOUR FROM o.order_ts) AS order_hour,
        EXTRACT(DOW FROM o.order_ts) AS order_day_of_week,
        
        CASE 
            WHEN EXTRACT(DOW FROM o.order_ts) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        CASE 
            WHEN o.coupon_code IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS has_coupon,
        
        -- Audit
        o.ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM orders_base o
    LEFT JOIN order_line_aggregates ola ON o.order_id = ola.order_id
)

SELECT * FROM orders_enriched