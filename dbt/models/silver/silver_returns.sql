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
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
),

-- Get order context (from staging - we only need basic fields)
orders AS (
    SELECT 
        order_id,
        customer_id,
        store_id,
        order_ts,
        order_dt_local
    FROM {{ ref('silver_orders') }}
),

-- Get product context (from staging - consistent with orders, we only need category/subcategory)
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
        
        -- Derived: Days from order to return
        CASE
            WHEN o.order_ts IS NOT NULL AND r.return_ts IS NOT NULL
            THEN EXTRACT(DAY FROM (r.return_ts - o.order_ts))
            ELSE NULL
        END AS days_to_return,
        
        -- Derived: Return reason category
        CASE
            WHEN LOWER(r.reason) LIKE '%defect%' OR LOWER(r.reason) LIKE '%broken%' THEN 'Defective'
            WHEN LOWER(r.reason) LIKE '%wrong%' OR LOWER(r.reason) LIKE '%incorrect%' THEN 'Wrong Item'
            WHEN LOWER(r.reason) LIKE '%size%' OR LOWER(r.reason) LIKE '%fit%' THEN 'Size Issue'
            WHEN LOWER(r.reason) LIKE '%change%' OR LOWER(r.reason) LIKE '%mind%' THEN 'Changed Mind'
            WHEN LOWER(r.reason) LIKE '%quality%' THEN 'Quality Issue'
            ELSE 'Other'
        END AS return_category,
        
        -- Derived: Return window
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
        
    FROM returns_raw r
    LEFT JOIN orders o ON r.order_id = o.order_id
    LEFT JOIN products p ON r.product_id = p.product_id
)

SELECT * FROM returns_enriched