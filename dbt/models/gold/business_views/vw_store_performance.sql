-- models/gold/business_views/vw_store_performance.sql
{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH dim_store AS (
    SELECT * FROM {{ ref('dim_stores') }}
),

fct_sales AS (
    SELECT * FROM {{ ref('fct_sales') }}
),

store_metrics AS (
    SELECT
        store_key,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_key) AS unique_customers,
        SUM(quantity) AS total_units_sold,
        SUM(net_amount) AS total_revenue,
        AVG(net_amount) AS avg_order_value
    FROM fct_sales
    GROUP BY store_key
)

SELECT
    -- Store identification
    s.store_id,
    s.store_code,
    s.store_name,
    
    -- Store attributes
    s.channel,
    s.geographic_region AS region,
    s.state,
    s.operational_status AS status,
    
    -- Store age
    s.store_age_years AS years_in_operation,
    
    -- Location
    s.latitude,
    s.longitude,
    s.climate_zone,
    
    -- Operational dates
    s.open_dt AS opening_date,
    CASE 
        WHEN s.operational_status = 'Closed' THEN s.close_dt
        ELSE NULL
    END AS closing_date,
    
    -- Sales metrics
    COALESCE(m.total_orders, 0) AS orders_count,
    COALESCE(m.unique_customers, 0) AS customers_count,
    COALESCE(m.total_units_sold, 0) AS units_sold,
    COALESCE(m.total_revenue, 0) AS total_revenue,
    COALESCE(m.avg_order_value, 0) AS average_order_value,
    
    -- Performance KPIs
    CASE
        WHEN COALESCE(m.total_revenue, 0) >= 100000 THEN 'Top Performer'
        WHEN COALESCE(m.total_revenue, 0) >= 50000 THEN 'Above Average'
        WHEN COALESCE(m.total_revenue, 0) >= 10000 THEN 'Average'
        WHEN COALESCE(m.total_revenue, 0) > 0 THEN 'Below Average'
        ELSE 'No Sales'
    END AS revenue_tier,
    
    -- Customer engagement
    CASE
        WHEN m.unique_customers IS NULL OR m.total_orders IS NULL THEN 0
        WHEN m.unique_customers = 0 THEN 0
        ELSE m.total_orders::FLOAT / m.unique_customers::FLOAT
    END AS orders_per_customer

FROM dim_store s
LEFT JOIN store_metrics m ON s.store_key = m.store_key