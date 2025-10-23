-- models/gold/facts/fct_shipments.sql
{{
    config(
        materialized='incremental',
        unique_key='shipment_key',
        on_schema_change='merge',
        schema='gold'
    )
}}

WITH silver_shipments AS (
    SELECT * FROM {{ ref('silver_shipments') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
),

silver_orders AS (
    SELECT 
        order_id,
        customer_id,
        store_id,
        order_ts,
        order_dt_local
    FROM {{ ref('silver_orders') }}
),

dim_customer AS (
    SELECT customer_key, customer_id FROM {{ ref('dim_customers') }}
),

dim_store AS (
    SELECT store_key, store_id FROM {{ ref('dim_stores') }}
),

dim_date_ship AS (
    SELECT date_key, date_day FROM {{ ref('dim_date') }}
),

dim_date_delivery AS (
    SELECT date_key, date_day FROM {{ ref('dim_date') }}
),

shipments_with_keys AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['sh.shipment_id']) }} AS shipment_key,
        
        -- Foreign keys
        c.customer_key,
        s.store_key,
        ds.date_key AS ship_date_key,
        dd.date_key AS delivery_date_key,
        
        -- Degenerate dimensions
        sh.shipment_id,
        sh.order_id,
        sh.carrier,
        
        -- Dates
        sh.shipped_at,
        sh.delivered_at,
        
        -- Measures (from silver layer calculations)
        sh.ship_cost AS shipping_cost,
        sh.delivery_days,
        sh.is_on_time_delivery,
        
        -- Delivery status
        CASE
            WHEN sh.delivered_at IS NULL THEN 'In Transit'
            WHEN sh.is_on_time_delivery = TRUE THEN 'On Time'
            WHEN sh.delivery_days <= 7 THEN 'Slightly Late'
            ELSE 'Delayed'
        END AS delivery_status,
        
        -- Carrier performance tier
        CASE
            WHEN sh.carrier = 'FedEx' THEN 'Premium'
            WHEN sh.carrier = 'UPS' THEN 'Premium'
            WHEN sh.carrier = 'USPS' THEN 'Standard'
            ELSE 'Other'
        END AS carrier_tier,
        
        -- Audit
        sh.ingestion_ts,
        CURRENT_TIMESTAMP AS fact_created_at
        
    FROM silver_shipments sh
    INNER JOIN silver_orders o ON sh.order_id = o.order_id
    INNER JOIN dim_customer c ON o.customer_id = c.customer_id
    INNER JOIN dim_store s ON o.store_id = s.store_id
    INNER JOIN dim_date_ship ds ON sh.shipped_at::DATE = ds.date_day
    LEFT JOIN dim_date_delivery dd ON sh.delivered_at::DATE = dd.date_day
)

SELECT * FROM shipments_with_keys
{% if is_incremental() %}
    WHERE shipment_key NOT IN (SELECT shipment_key FROM {{ this }})
{% endif %}