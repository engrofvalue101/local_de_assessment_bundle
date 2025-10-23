-- models/silver/silver_returns.sql
{{
    config(
        materialized='incremental',
        unique_key='return_id',
        on_schema_change='merge',
        schema='silver'
    )
}}

WITH returns_raw AS (
    SELECT * FROM {{ ref('stg_returns') }}
    {% if is_incremental() %}
    WHERE ingestion_ts > (
        SELECT COALESCE(MAX(ingestion_ts), '1900-01-01'::TIMESTAMP) 
        FROM {{ this }}
    )
    {% endif %}
),
-- Get order context (from staging - we only need basic fields)
returns_with_validation AS (
    SELECT
        *,
        
        -- VALIDATION FLAGS (based on staging tests)
        CASE WHEN return_id IS NULL THEN TRUE ELSE FALSE END AS has_null_return_id,
        CASE WHEN order_id IS NULL THEN TRUE ELSE FALSE END AS has_null_order_id,
        CASE WHEN product_id IS NULL THEN TRUE ELSE FALSE END AS has_null_product_id,
        CASE WHEN return_ts IS NULL THEN TRUE ELSE FALSE END AS has_null_return_ts,
        
        -- Relationship checks (will be validated via staging tests)
        CASE WHEN order_id IS NOT NULL THEN FALSE ELSE FALSE END AS has_invalid_order_relationship,
        CASE WHEN product_id IS NOT NULL THEN FALSE ELSE FALSE END AS has_invalid_product_relationship,
        
        -- Additional quality checks
        CASE WHEN qty IS NULL OR qty <= 0 THEN TRUE ELSE FALSE END AS has_invalid_quantity,
        CASE WHEN return_ts > CURRENT_TIMESTAMP THEN TRUE ELSE FALSE END AS has_future_return_ts,
        
        -- OVERALL VALIDITY FLAG
        CASE 
            WHEN return_id IS NULL THEN FALSE
            WHEN order_id IS NULL THEN FALSE
            WHEN product_id IS NULL THEN FALSE
            WHEN return_ts IS NULL THEN FALSE
            WHEN qty IS NULL OR qty <= 0 THEN FALSE
            WHEN return_ts > CURRENT_TIMESTAMP THEN FALSE
            ELSE TRUE
        END AS is_valid_record,
        
        -- QUALITY ISSUE TYPE
        CASE
            WHEN return_id IS NULL THEN 'Missing Return ID'
            WHEN order_id IS NULL THEN 'Missing Order ID'
            WHEN product_id IS NULL THEN 'Missing Product ID'
            WHEN return_ts IS NULL THEN 'Missing Return Timestamp'
            WHEN qty IS NULL OR qty <= 0 THEN 'Invalid Quantity'
            WHEN return_ts > CURRENT_TIMESTAMP THEN 'Future Return Timestamp'
            ELSE NULL
        END AS quality_issue_type
        
    FROM returns_raw
),

-- Get order context (only valid orders)
orders AS (
    SELECT 
        order_id,
        customer_id,
        store_id,
        order_ts,
        order_dt_local
    FROM {{ ref('silver_orders') }}
    WHERE is_valid_record = TRUE
),

-- Get product context
products AS (
    SELECT
        product_id,
        category,
        subcategory
    FROM {{ ref('silver_products') }}
),

-- Enrichment
returns_enriched AS (
    SELECT
        r.return_id,
        r.order_id,
        r.product_id,
        o.customer_id,
        o.store_id,
        r.return_ts,
        CAST(r.return_ts AS DATE) AS return_date,
        r.qty AS return_qty,
        r.reason AS return_reason,
        o.order_ts,
        o.order_dt_local,
        p.category,
        p.subcategory,
        
        -- Quality flags
        r.has_null_return_id,
        r.has_null_order_id,
        r.has_null_product_id,
        r.has_null_return_ts,
        r.has_invalid_order_relationship,
        r.has_invalid_product_relationship,
        r.has_invalid_quantity,
        r.has_future_return_ts,
        r.is_valid_record,
        r.quality_issue_type,
        
        -- Derived fields
        CASE
            WHEN o.order_ts IS NOT NULL AND r.return_ts IS NOT NULL
            THEN EXTRACT(DAY FROM (r.return_ts - o.order_ts))
            ELSE NULL
        END AS days_to_return,
        
        CASE
            WHEN LOWER(r.reason) LIKE '%defect%' OR LOWER(r.reason) LIKE '%broken%' THEN 'Defective'
            WHEN LOWER(r.reason) LIKE '%wrong%' OR LOWER(r.reason) LIKE '%incorrect%' THEN 'Wrong Item'
            WHEN LOWER(r.reason) LIKE '%size%' OR LOWER(r.reason) LIKE '%fit%' THEN 'Size Issue'
            WHEN LOWER(r.reason) LIKE '%change%' OR LOWER(r.reason) LIKE '%mind%' THEN 'Changed Mind'
            WHEN LOWER(r.reason) LIKE '%quality%' THEN 'Quality Issue'
            ELSE 'Other'
        END AS return_category,
        
        CASE
            WHEN EXTRACT(DAY FROM (r.return_ts - o.order_ts)) <= 7 THEN 'Within 1 Week'
            WHEN EXTRACT(DAY FROM (r.return_ts - o.order_ts)) <= 14 THEN 'Within 2 Weeks'
            WHEN EXTRACT(DAY FROM (r.return_ts - o.order_ts)) <= 30 THEN 'Within 1 Month'
            WHEN EXTRACT(DAY FROM (r.return_ts - o.order_ts)) > 30 THEN 'After 1 Month'
            ELSE 'Unknown'
        END AS return_window,
        
        -- Audit
        r.ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM returns_with_validation r
    LEFT JOIN orders o ON r.order_id = o.order_id
    LEFT JOIN products p ON r.product_id = p.product_id
)

SELECT * FROM returns_enriched