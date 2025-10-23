-- models/gold/business_views/vw_product_performance.sql
{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH dim_product AS (
    SELECT * FROM {{ ref('dim_product_scd') }}
    WHERE is_current = TRUE  -- Only current product versions
),

fct_sales AS (
    SELECT * FROM {{ ref('fct_sales') }}
),

product_metrics AS (
    SELECT
        product_key,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS total_units_sold,
        SUM(gross_amount) AS total_revenue,
        SUM(discount_amount) AS total_discounts_given,
        SUM(net_amount) AS total_net_revenue,
        AVG(unit_price) AS avg_selling_price,
        AVG(discount_pct) AS avg_discount_pct,
        COUNT(DISTINCT CASE WHEN has_discount = TRUE THEN order_id END) AS discounted_orders
    FROM fct_sales
    GROUP BY product_key
)

SELECT
    -- Product identification
    p.product_id,
    p.sku,
    p.product_name,
    
    -- Product attributes
    p.category,
    p.subcategory,
    p.current_price AS list_price,
    p.currency,
    p.price_tier,
    
    -- Product lifecycle
    p.introduced_dt AS launch_date,
    p.product_status,
    CASE 
        WHEN p.is_discontinued = TRUE THEN p.discontinued_dt
        ELSE NULL
    END AS discontinued_date,
    
    -- Sales metrics
    COALESCE(m.total_orders, 0) AS orders_count,
    COALESCE(m.total_units_sold, 0) AS units_sold,
    COALESCE(m.total_revenue, 0) AS gross_revenue,
    COALESCE(m.total_net_revenue, 0) AS net_revenue,
    COALESCE(m.total_discounts_given, 0) AS total_discounts,
    
    -- Pricing metrics
    COALESCE(m.avg_selling_price, 0) AS average_price,
    COALESCE(m.avg_discount_pct, 0) * 100 AS average_discount_percent,
    
    -- Performance indicators
    CASE
        WHEN COALESCE(m.total_units_sold, 0) = 0 THEN 'No Sales'
        WHEN COALESCE(m.total_units_sold, 0) < 10 THEN 'Low Performer'
        WHEN COALESCE(m.total_units_sold, 0) < 100 THEN 'Average Performer'
        WHEN COALESCE(m.total_units_sold, 0) < 500 THEN 'Good Performer'
        ELSE 'Top Performer'
    END AS sales_performance_tier,
    
    CASE
        WHEN COALESCE(m.discounted_orders, 0)::FLOAT / NULLIF(m.total_orders, 0)::FLOAT > 0.7 THEN 'Discount Dependent'
        WHEN COALESCE(m.discounted_orders, 0)::FLOAT / NULLIF(m.total_orders, 0)::FLOAT > 0.3 THEN 'Occasionally Discounted'
        ELSE 'Full Price Seller'
    END AS discount_dependency,
    
    -- Stock status
    CASE
        WHEN p.is_discontinued = TRUE THEN 'Discontinued'
        WHEN COALESCE(m.total_orders, 0) = 0 THEN 'No Sales Yet'
        ELSE 'Active'
    END AS stock_status

FROM dim_product p
LEFT JOIN product_metrics m ON p.product_key = m.product_key