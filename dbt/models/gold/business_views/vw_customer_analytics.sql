-- models/gold/business_views/vw_customer_analytics.sql
{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH dim_customer AS (
    SELECT * FROM {{ ref('dim_customers') }}
)

SELECT
    -- Customer identification
    customer_id,
    first_name || ' ' || last_name AS customer_name,
    email,
    phone,
    
    -- Location
    city,
    state_region AS state,
    country_code AS country,
    
    -- Demographics
    customer_age AS age,
    age_group,
    
    -- Account information
    join_ts AS join_date,
    customer_lifetime_days AS days_as_customer,
    customer_tenure_segment AS tenure,
    is_vip,
    customer_segment AS original_segment,
    
    -- Purchase behavior
    total_orders,
    lifetime_gross_revenue,
    lifetime_net_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    days_since_last_order,
    
    -- Segmentation
    value_segment,
    recency_status,
    
    -- RFM-style categorization
    CASE
        WHEN recency_status = 'Active' AND is_vip = TRUE THEN 'Champions'
        WHEN recency_status = 'Active' AND lifetime_net_revenue >= 5000 THEN 'Loyal Customers'
        WHEN recency_status IN ('Active', 'Recent') AND total_orders >= 3 THEN 'Potential Loyalists'
        WHEN recency_status = 'Active' AND total_orders <= 2 THEN 'New Customers'
        WHEN recency_status IN ('Lapsing', 'At Risk') AND lifetime_net_revenue >= 5000 THEN 'At Risk High Value'
        WHEN recency_status IN ('Lapsing', 'At Risk') THEN 'At Risk'
        WHEN recency_status = 'Inactive' AND lifetime_net_revenue >= 5000 THEN 'Lost High Value'
        WHEN recency_status = 'Inactive' THEN 'Lost'
        ELSE 'Other'
    END AS customer_lifecycle_stage,
    
    -- Privacy flag
    gdpr_consent AS marketing_consent

FROM dim_customer
WHERE customer_id IS NOT NULL