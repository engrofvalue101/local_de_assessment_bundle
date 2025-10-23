-- models/gold/business_views/vw_financial_summary.sql
{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH fct_sales AS (
    SELECT * FROM {{ ref('fct_sales') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

dim_customer AS (
    SELECT 
        customer_key,
        value_segment,
        is_vip
    FROM {{ ref('dim_customers') }}
),

dim_product AS (
    SELECT 
        product_key,
        category,
        subcategory
    FROM {{ ref('dim_product_scd') }}
    WHERE is_current = TRUE
),

dim_store AS (
    SELECT 
        store_key,
        store_name,
        channel,
        geographic_region
    FROM {{ ref('dim_stores') }}
)

SELECT
    -- Time dimensions
    d.date_day AS sale_date,
    d.year,
    d.quarter,
    d.month_name AS month,
    d.year_month,
    d.year_quarter,
    d.day_name AS day_of_week,
    d.is_weekend,
    
    -- Product dimensions
    p.category AS product_category,
    p.subcategory AS product_subcategory,
    
    -- Store dimensions
    st.store_name,
    st.channel AS sales_channel,
    st.geographic_region AS region,
    
    -- Customer dimensions
    c.value_segment AS customer_segment,
    c.is_vip AS vip_customer,
    
    -- Transaction details
    s.order_id,
    s.line_number,
    --s.payment_method,
    s.quantity AS units_sold,
    
    -- Financial metrics
    s.unit_price,
    s.gross_amount AS gross_sales,
    s.discount_amount AS discounts,
    s.discount_pct * 100 AS discount_percent,
    s.net_amount AS net_sales,
    s.tax_amount AS tax,
    s.total_amount AS total_sales,
    
    -- Margin calculations (assuming 40% COGS)
    s.net_amount * 0.40 AS estimated_cogs,
    s.net_amount * 0.60 AS estimated_gross_profit,
    (s.net_amount * 0.60 / NULLIF(s.net_amount, 0)) * 100 AS estimated_margin_percent,
    
    -- Discount indicators
    s.has_discount AS sale_was_discounted,
    s.discount_tier,
    
    -- Performance flags
    CASE 
        WHEN s.net_amount >= 500 THEN 'High Value Transaction'
        WHEN s.net_amount >= 100 THEN 'Medium Value Transaction'
        ELSE 'Low Value Transaction'
    END AS transaction_value_tier

FROM fct_sales s
INNER JOIN dim_date d ON s.date_key = d.date_key
INNER JOIN dim_customer c ON s.customer_key = c.customer_key
INNER JOIN dim_product p ON s.product_key = p.product_key
INNER JOIN dim_store st ON s.store_key = st.store_key