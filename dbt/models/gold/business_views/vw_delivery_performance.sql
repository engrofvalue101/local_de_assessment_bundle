-- models/gold/business_views/vw_delivery_performance.sql
{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH fct_shipments AS (
    SELECT * FROM {{ ref('fct_shipments') }}
),

dim_customer AS (
    SELECT 
        customer_key,
        customer_id,
        first_name || ' ' || last_name AS customer_name,
        city,
        state_region,
        country_code
    FROM {{ ref('dim_customers') }}
),

dim_store AS (
    SELECT 
        store_key,
        store_id,
        store_name,
        channel
    FROM {{ ref('dim_stores') }}
),

dim_date_ship AS (
    SELECT date_key, date_day FROM {{ ref('dim_date') }}
),

dim_date_delivery AS (
    SELECT date_key, date_day FROM {{ ref('dim_date') }}
)

SELECT
    -- Shipment identification
    sh.shipment_id,
    sh.order_id,
    
    -- Customer information
    c.customer_id,
    c.customer_name,
    c.city AS customer_city,
    c.state_region AS customer_state,
    c.country_code AS customer_country,
    
    -- Store information
    st.store_name,
    st.channel,
    
    -- Shipping details
    sh.carrier,
    sh.carrier_tier,
    
    -- Dates (business-friendly)
    ds.date_day AS ship_date,
    dd.date_day AS delivery_date,
    
    -- Timing metrics
    sh.delivery_days AS days_to_deliver,
    sh.is_on_time_delivery AS delivered_on_time,
    
    -- Cost
    sh.shipping_cost,
    
    -- Status
    sh.delivery_status,
    
    -- Performance indicators
    CASE
        WHEN sh.delivery_days IS NULL THEN 'In Transit'
        WHEN sh.delivery_days <= 2 THEN 'Express'
        WHEN sh.delivery_days <= 5 THEN 'Standard'
        WHEN sh.delivery_days <= 10 THEN 'Slow'
        ELSE 'Very Slow'
    END AS delivery_speed,
    
    CASE
        WHEN sh.is_on_time_delivery = TRUE THEN 'On Time'
        WHEN sh.delivery_days > 5 AND sh.delivery_days <= 7 THEN 'Slightly Late'
        WHEN sh.delivery_days > 7 THEN 'Late'
        ELSE 'Unknown'
    END AS timeliness

FROM fct_shipments sh
INNER JOIN dim_customer c ON sh.customer_key = c.customer_key
INNER JOIN dim_store st ON sh.store_key = st.store_key
INNER JOIN dim_date_ship ds ON sh.ship_date_key = ds.date_key
LEFT JOIN dim_date_delivery dd ON sh.delivery_date_key = dd.date_key