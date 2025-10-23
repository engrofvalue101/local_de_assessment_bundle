-- models/gold/business_views/vw_sales_summary.sql
{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH fct_sales AS (
    SELECT * FROM {{ ref('fct_sales') }}
),

dim_customer AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

dim_product AS (
    SELECT * FROM {{ ref('dim_product_scd') }}
),

dim_store AS (
    SELECT * FROM {{ ref('dim_stores') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    -- Order information
    f.order_id,
    f.line_number,
    
    -- Date information (business-friendly)
    d.date_day AS order_date,
    d.year_month AS year_month,
    d.year AS year,
    d.quarter AS quarter,
    d.month_name AS month,
    d.day_name AS day_of_week,
    
    -- Customer information (no technical keys)
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    c.city AS customer_city,
    c.state_region AS customer_state,
    c.country_code AS customer_country,
    c.value_segment AS customer_segment,
    c.is_vip,
    c.customer_age,
    
    -- Product information (current version only)
    p.product_id,
    p.product_name,
    p.category AS product_category,
    p.subcategory AS product_subcategory,
    p.price_tier,
    
    -- Store information
    s.store_id,
    s.store_name,
    s.channel AS store_channel,
    s.region AS store_region,
    s.state AS store_state,
    
    -- Transaction details
    --f.payment_method,
    f.channel AS order_channel,
    f.quantity,
    f.unit_price,
    
    -- Financial metrics
    f.gross_amount,
    f.discount_amount,
    f.discount_pct AS discount_percentage,
    f.net_amount,
    f.tax_amount,
    f.total_amount,
    
    -- Flags
    f.has_discount,
    f.discount_tier
    
FROM fct_sales f
INNER JOIN dim_customer c ON f.customer_key = c.customer_key
INNER JOIN dim_product p ON f.product_key = p.product_key
INNER JOIN dim_store s ON f.store_key = s.store_key
INNER JOIN dim_date d ON f.date_key = d.date_key