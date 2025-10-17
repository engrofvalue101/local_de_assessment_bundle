-- models/silver/silver_shipments.sql
{{
    config(
        materialized='incremental',
        unique_key='shipment_id',
        on_schema_change='merge',
        schema='silver'
    )
}}

WITH shipments_raw AS (
    SELECT * FROM {{ ref('stg_shipments') }}
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

-- Enrichment
shipments_enriched AS (
    SELECT
        s.shipment_id,
        s.order_id,
        o.customer_id,
        o.store_id,
        s.carrier,
        s.shipped_at,
        CAST(s.shipped_at AS DATE) AS shipped_date,
        s.delivered_at,
        CAST(s.delivered_at AS DATE) AS delivered_date,
        s.ship_cost,
        o.channel,
        o.currency,
        o.order_ts,
        o.order_dt_local,
        
        -- Derived: Days from order to shipment
        CASE
            WHEN o.order_ts IS NOT NULL AND s.shipped_at IS NOT NULL
            THEN EXTRACT(DAY FROM (s.shipped_at - o.order_ts))
            ELSE NULL
        END AS days_to_ship,
        
        -- Derived: Delivery time (days from shipment to delivery)
        CASE
            WHEN s.shipped_at IS NOT NULL AND s.delivered_at IS NOT NULL
            THEN EXTRACT(DAY FROM (s.delivered_at - s.shipped_at))
            ELSE NULL
        END AS delivery_days,
        
        -- Derived: Total days from order to delivery
        CASE
            WHEN o.order_ts IS NOT NULL AND s.delivered_at IS NOT NULL
            THEN EXTRACT(DAY FROM (s.delivered_at - o.order_ts))
            ELSE NULL
        END AS total_fulfillment_days,
        
        -- Derived: Shipment status
        CASE
            WHEN s.delivered_at IS NOT NULL THEN 'Delivered'
            WHEN s.shipped_at IS NOT NULL THEN 'In Transit'
            ELSE 'Pending'
        END AS shipment_status,
        
        -- Derived: Carrier category
        CASE
            WHEN LOWER(s.carrier) IN ('fedex', 'ups', 'dhl', 'usps') THEN 'Major Carrier'
            WHEN LOWER(s.carrier) IN ('australia post', 'aus post') THEN 'National Post'
            WHEN s.carrier IS NULL THEN 'Unknown'
            ELSE 'Other Carrier'
        END AS carrier_category,
        
        -- Derived: Shipping cost band
        CASE
            WHEN s.ship_cost IS NULL THEN 'Unknown'
            WHEN s.ship_cost = 0 THEN 'Free Shipping'
            WHEN s.ship_cost > 0 AND s.ship_cost <= 5 THEN 'Economy'
            WHEN s.ship_cost > 5 AND s.ship_cost <= 15 THEN 'Standard'
            WHEN s.ship_cost > 15 AND s.ship_cost <= 30 THEN 'Express'
            WHEN s.ship_cost > 30 THEN 'Premium'
            ELSE 'Unknown'
        END AS shipping_cost_band,
        
        -- Derived: Delivery speed category (based on delivery_days)
        CASE
            WHEN s.delivered_at IS NULL THEN 'Not Delivered'
            WHEN EXTRACT(DAY FROM (s.delivered_at - s.shipped_at)) <= 1 THEN 'Same/Next Day'
            WHEN EXTRACT(DAY FROM (s.delivered_at - s.shipped_at)) <= 2 THEN '2-Day'
            WHEN EXTRACT(DAY FROM (s.delivered_at - s.shipped_at)) <= 5 THEN 'Standard (3-5 Days)'
            WHEN EXTRACT(DAY FROM (s.delivered_at - s.shipped_at)) <= 10 THEN 'Extended (6-10 Days)'
            WHEN EXTRACT(DAY FROM (s.delivered_at - s.shipped_at)) > 10 THEN 'Delayed (10+ Days)'
            ELSE 'Unknown'
        END AS delivery_speed_category,
        
        -- Derived: On-time delivery flag (assuming 5 days is standard)
        CASE
            WHEN s.delivered_at IS NOT NULL 
                AND EXTRACT(DAY FROM (s.delivered_at - s.shipped_at)) <= 5 
            THEN TRUE
            WHEN s.delivered_at IS NOT NULL 
                AND EXTRACT(DAY FROM (s.delivered_at - s.shipped_at)) > 5 
            THEN FALSE
            ELSE NULL
        END AS is_on_time_delivery,
        
        -- Derived: Time components for shipped_at
        EXTRACT(HOUR FROM s.shipped_at) AS shipped_hour,
        EXTRACT(DOW FROM s.shipped_at) AS shipped_day_of_week,
        
        -- Derived: Is shipped on weekend
        CASE 
            WHEN EXTRACT(DOW FROM s.shipped_at) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend_shipment,
        
        -- Audit
        s.ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM shipments_raw s
    LEFT JOIN orders o ON s.order_id = o.order_id
)

SELECT * FROM shipments_enriched